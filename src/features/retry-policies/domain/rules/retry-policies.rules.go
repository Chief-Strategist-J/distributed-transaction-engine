package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
)

func ComputeFingerprint(statusCode int, errMsg string) string {
	normalized := strings.TrimSpace(errMsg)
	raw := fmt.Sprintf("%d|%s", statusCode, normalized)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func IsDeterministicError(fingerprintHistory []string, currentFingerprint string) bool {
	if len(fingerprintHistory) < 2 {
		return false
	}
	consecutive := 1
	for i := len(fingerprintHistory) - 1; i >= 0; i-- {
		if fingerprintHistory[i] == currentFingerprint {
			consecutive++
			if consecutive >= 3 {
				return true
			}
		} else {
			break
		}
	}
	return false
}

func ClassifyError(
	statusCode int,
	errMsg string,
	attempt int,
	history []string,
) contracts.ErrorClass {
	currentFp := ComputeFingerprint(statusCode, errMsg)
	if IsDeterministicError(history, currentFp) {
		return contracts.ClassDeterministicError
	}

	if statusCode == 0 ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "TLS handshake") {
		return contracts.ClassNotSent
	}

	switch statusCode {
	case 429, 503:
		return contracts.ClassTransientNotExecuted
	case 401:
		return contracts.ClassCredential
	case 404:
		return contracts.ClassCausalWait
	case 409:
		if attempt > 1 {
			return contracts.ClassTransientNotExecuted
		}
		return contracts.ClassRejected
	case 400, 422:
		return contracts.ClassRejected
	case 500, 502, 504:
		return contracts.ClassAmbiguous
	default:
		if statusCode >= 500 {
			return contracts.ClassAmbiguous
		}
		return contracts.ClassRejected
	}
}

func CalculateBackoff(
	attempt int,
	policy contracts.RetryPolicyConfig,
	retryAfterDelay time.Duration,
	hasRetryAfter bool,
) time.Duration {
	attemptClamped := attempt
	if attemptClamped > 30 {
		attemptClamped = 30
	}

	base := policy.BaseIntervalMillis
	capMillis := policy.MaxIntervalMillis
	floor := base / 2
	if floor < 10 {
		floor = 10
	}

	var sleepMillis int
	switch policy.Algorithm {
	case contracts.AlgorithmEqualJitter:
		multiplier := 1 << attemptClamped
		exponential := base * multiplier
		if exponential > capMillis {
			exponential = capMillis
		}
		half := exponential / 2
		if half < 1 {
			half = 1
		}
		sleepMillis = half + rand.Intn(half)

	case contracts.AlgorithmLinear:
		linear := base * attemptClamped
		if linear > capMillis {
			linear = capMillis
		}
		jitterRange := base / 2
		if jitterRange < 1 {
			jitterRange = 1
		}
		sleepMillis = linear + rand.Intn(jitterRange)

	case contracts.AlgorithmFullJitter:
		fallthrough
	default:
		multiplier := 1 << attemptClamped
		exponential := base * multiplier
		if exponential > capMillis {
			exponential = capMillis
		}
		rangeSpan := exponential - floor
		if rangeSpan < 1 {
			rangeSpan = 1
		}
		sleepMillis = floor + rand.Intn(rangeSpan)
	}

	computed := time.Duration(sleepMillis) * time.Millisecond
	if hasRetryAfter && retryAfterDelay > computed {
		return retryAfterDelay
	}
	return computed
}

func EvaluateDecision(
	class contracts.ErrorClass,
	attempt int,
	firstAttemptTime time.Time,
	effectiveDeadline *time.Time,
	capability contracts.StepCapability,
	position contracts.SagaPosition,
	policy contracts.RetryPolicyConfig,
	computedDelay time.Duration,
	now time.Time,
) contracts.RetryDecision {
	if class == contracts.ClassDeterministicError {
		return contracts.RetryDecision{
			Action:     contracts.ActionNeedsAttention,
			ErrorClass: class,
			Reason:     "deterministic error detected across consecutive attempts",
		}
	}

	if class == contracts.ClassRejected {
		if position == contracts.PositionCompensable {
			return contracts.RetryDecision{
				Action:     contracts.ActionHaltCompensate,
				ErrorClass: class,
				Reason:     "terminal business rejection on compensable step",
			}
		}
		return contracts.RetryDecision{
			Action:     contracts.ActionNeedsAttention,
			ErrorClass: class,
			Reason:     "terminal business rejection on pivot or post-pivot step",
		}
	}

	if attempt >= policy.MaxAttempts {
		if position == contracts.PositionCompensable {
			return contracts.RetryDecision{
				Action:     contracts.ActionHaltCompensate,
				ErrorClass: class,
				Reason:     "retry attempt budget exhausted",
			}
		}
		return contracts.RetryDecision{
			Action:     contracts.ActionNeedsAttention,
			ErrorClass: class,
			Reason:     "retry attempt budget exhausted on post-pivot step",
		}
	}

	elapsed := now.Sub(firstAttemptTime)
	maxElapsed := time.Duration(policy.MaxElapsedSeconds) * time.Second
	if elapsed >= maxElapsed {
		if position == contracts.PositionCompensable {
			return contracts.RetryDecision{
				Action:     contracts.ActionHaltCompensate,
				ErrorClass: class,
				Reason:     "retry max elapsed time budget exhausted",
			}
		}
		return contracts.RetryDecision{
			Action:     contracts.ActionNeedsAttention,
			ErrorClass: class,
			Reason:     "retry max elapsed time exhausted on post-pivot step",
		}
	}

	if effectiveDeadline != nil {
		if now.Add(computedDelay).After(*effectiveDeadline) {
			if position == contracts.PositionCompensable {
				return contracts.RetryDecision{
					Action:     contracts.ActionHaltCompensate,
					ErrorClass: class,
					Reason:     "computed retry delay exceeds effective step deadline",
				}
			}
			return contracts.RetryDecision{
				Action:     contracts.ActionNeedsAttention,
				ErrorClass: class,
				Reason:     "computed retry delay exceeds effective deadline on post-pivot step",
			}
		}
	}

	if class == contracts.ClassAmbiguous && !capability.IdempotentByKey && !capability.StatusQuery {
		return contracts.RetryDecision{
			Action:     contracts.ActionNeedsAttention,
			ErrorClass: class,
			Reason:     "ambiguous timeout on non-idempotent worker without status query capability",
		}
	}

	return contracts.RetryDecision{
		Action:      contracts.ActionRetry,
		Delay:       computedDelay,
		DelayMillis: computedDelay.Milliseconds(),
		ErrorClass:  class,
		Reason:      "retry scheduled with backoff delay",
	}
}


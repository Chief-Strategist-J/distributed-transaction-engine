package rules

import (
	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"strings"
	"time"
)

type ErrorClassification string

const (
	ErrorRetriable    ErrorClassification = "RETRIABLE"
	ErrorNonRetriable ErrorClassification = "NON_RETRIABLE"
)

const (
	MaxPublishAttempts = 8
	maxRelayBackoff    = 60 * time.Second
	maxBackoffExponent = 6
)

func RelayBackoff(attemptCount int) time.Duration {
	defer observability.TraceEnter(nil)()
	exponent := attemptCount
	if exponent > maxBackoffExponent {
		exponent = maxBackoffExponent
	}
	if exponent < 0 {
		exponent = 0
	}
	backoff := time.Second << uint(exponent)
	if backoff > maxRelayBackoff {
		return maxRelayBackoff
	}
	return backoff
}

func ExceededPublishAttempts(attemptCount int) bool {
	defer observability.TraceEnter(nil)()
	return attemptCount >= MaxPublishAttempts
}

func ClassifyPublishError(err error) ErrorClassification {
	defer observability.TraceEnter(nil)()
	if err == nil {
		return ErrorRetriable
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "record too large") ||
		strings.Contains(msg, "authorization failed") ||
		strings.Contains(msg, "invalid message") ||
		strings.Contains(msg, "invalid record") {
		return ErrorNonRetriable
	}
	return ErrorRetriable
}

func ValidateSequenceProgression(currentSeq int64, nextSeq int64) bool {
	defer observability.TraceEnter(nil)()
	return nextSeq == currentSeq+1
}

func CanTransitionOutboxStatus(from contracts.OutboxStatus, to contracts.OutboxStatus) bool {
	defer observability.TraceEnter(nil)()
	switch from {
	case contracts.OutboxStatusPending:
		return to == contracts.OutboxStatusInFlight
	case contracts.OutboxStatusInFlight:
		return to == contracts.OutboxStatusPublished || to == contracts.OutboxStatusPending || to == contracts.OutboxStatusParked
	case contracts.OutboxStatusPublished:
		return false
	case contracts.OutboxStatusParked:
		return to == contracts.OutboxStatusPending
	default:
		return false
	}
}

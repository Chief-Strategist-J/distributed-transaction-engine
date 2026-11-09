package rules

import (
	"crypto/rand"
	"math/big"
	"strings"
	"time"

	"distributed-transaction-engine/src/features/compensation-retry/contracts"
)

type CompensationRetryRules struct{}

func NewCompensationRetryRules() *CompensationRetryRules {
	return &CompensationRetryRules{}
}

func (r *CompensationRetryRules) ClassifyResult(statusCode int, errMessage string) contracts.OutcomeClass {
	if statusCode >= 200 && statusCode < 300 {
		return contracts.OutcomeDone
	}

	msgLower := strings.ToLower(errMessage)

	if statusCode == 504 || strings.Contains(msgLower, "timeout") || strings.Contains(msgLower, "deadline exceeded") {
		return contracts.OutcomeAmbiguous
	}

	if statusCode == 400 {
		return contracts.OutcomeContractDefect
	}

	if statusCode == 401 || statusCode == 403 {
		return contracts.OutcomeCredential
	}

	if statusCode == 404 || statusCode == 409 || strings.Contains(msgLower, "cannot cancel") || strings.Contains(msgLower, "already shipped") {
		return contracts.OutcomeBusinessAlternative
	}

	if statusCode == 429 {
		return contracts.OutcomeRateLimited
	}

	if statusCode >= 500 && statusCode <= 599 {
		return contracts.OutcomeRetry
	}

	return contracts.OutcomeNeedsAttention
}

func (r *CompensationRetryRules) CalculateBackoff(attempt int, baseMillis, maxMillis int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}

	if baseMillis < 100 {
		baseMillis = 500
	}
	if maxMillis < baseMillis {
		maxMillis = 60000
	}

	if attempt < 1 {
		attempt = 1
	}
	if attempt > 30 {
		attempt = 30
	}

	floorMillis := baseMillis / 2
	ceiling := int64(baseMillis) * (1 << (attempt - 1))
	if ceiling > int64(maxMillis) || ceiling < 0 {
		ceiling = int64(maxMillis)
	}

	span := ceiling - int64(floorMillis)
	if span <= 0 {
		return time.Duration(floorMillis) * time.Millisecond
	}

	randomBig, err := rand.Int(rand.Reader, big.NewInt(span))
	if err != nil {
		return time.Duration(ceiling) * time.Millisecond
	}

	computed := int64(floorMillis) + randomBig.Int64()
	return time.Duration(computed) * time.Millisecond
}

func (r *CompensationRetryRules) DetermineEscalation(
	now time.Time,
	firstAttempt time.Time,
	deadline time.Time,
	attempt int,
	maxAttempts int,
) (contracts.EscalationLevel, bool) {
	if !now.Before(deadline) {
		return contracts.EscalationLevel3Missed, true
	}

	if attempt >= maxAttempts {
		return contracts.EscalationLevel2Deadline, true
	}

	remainingToDeadline := deadline.Sub(now)
	if remainingToDeadline <= 30*time.Minute {
		return contracts.EscalationLevel2Deadline, false
	}

	totalHorizon := deadline.Sub(firstAttempt)
	elapsed := now.Sub(firstAttempt)
	if totalHorizon > 0 && elapsed >= totalHorizon/2 {
		return contracts.EscalationLevel1Early, false
	}

	return contracts.EscalationNone, false
}

func (r *CompensationRetryRules) ShouldThrottleTenant(tenantUsageRatio float64) bool {
	return tenantUsageRatio > 0.20
}


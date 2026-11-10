package rules

import (
	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"time"
)

type FencingRules struct{}

func NewFencingRules() *FencingRules {
	return &FencingRules{}
}

func (r *FencingRules) EvaluateFencingBarrier(storedToken int64, incomingToken int64) contracts.FencingDecision {
	defer observability.TraceEnter(nil)()
	if incomingToken >= storedToken {
		return contracts.FencingDecision{
			Allowed:        true,
			StoredToken:    storedToken,
			AttemptedToken: incomingToken,
			Reason:         "fencing barrier check passed",
		}
	}

	return contracts.FencingDecision{
		Allowed:        false,
		StoredToken:    storedToken,
		AttemptedToken: incomingToken,
		Reason:         "stale fencing token; zombie write suppressed",
		ViolationType:  "STALE_TOKEN",
	}
}

func (r *FencingRules) ComputeLeaseExpiry(now time.Time, durationMs int) time.Time {
	defer observability.TraceEnter(nil)()
	if durationMs < 1000 {
		durationMs = 15000
	}
	return now.Add(time.Duration(durationMs) * time.Millisecond)
}

func (r *FencingRules) IsLeaseExpired(now time.Time, expiresAt time.Time) bool {
	defer observability.TraceEnter(nil)()
	return !now.Before(expiresAt)
}

func (r *FencingRules) HasSufficientHeadroom(now time.Time, expiresAt time.Time, expectedDurationMs int) bool {
	defer observability.TraceEnter(nil)()
	requiredHeadroom := time.Duration(expectedDurationMs*2) * time.Millisecond
	remaining := expiresAt.Sub(now)
	return remaining >= requiredHeadroom
}

func (r *FencingRules) ClassifyRejection(storedToken int64, incomingToken int64, isExpired bool) string {
	defer observability.TraceEnter(nil)()
	if incomingToken < storedToken {
		return "STALE_TOKEN"
	}
	if isExpired {
		return "LEASE_EXPIRED"
	}
	return "UNAUTHORIZED_HOLDER"
}

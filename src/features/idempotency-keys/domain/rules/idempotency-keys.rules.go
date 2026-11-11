package rules

import (
	"crypto/sha256"
	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"encoding/hex"
	"fmt"
	"time"
)

func EvaluateExistingRecord(
	existing *contracts.IdempotencyRecord,
	incomingFingerprint string,
	now time.Time,
) contracts.AcquireOutcome {
	defer observability.TraceEnter(nil)()
	if existing == nil {
		return contracts.OutcomeAcquired
	}

	if existing.RequestFingerprint != incomingFingerprint {
		return contracts.OutcomeFingerprintMismatch
	}

	switch existing.CurrentState {
	case contracts.StateCompleted, contracts.StateRejectedFinal:
		return contracts.OutcomeReplay

	case contracts.StateInProgress:
		if IsLeaseExpired(existing, now) {
			return contracts.OutcomeAcquired
		}
		return contracts.OutcomeConflictInProgress

	case contracts.StateUnknown:
		return contracts.OutcomeConflictInProgress

	default:
		return contracts.OutcomeConflictInProgress
	}
}

const DefaultLeaseDuration = 30 * time.Second

func IsLeaseExpired(record *contracts.IdempotencyRecord, now time.Time) (spanResult0 bool) {
	defer observability.TraceEnter(nil)()
	if record.LeaseUntil != nil {
		return now.After(*record.LeaseUntil)
	}
	return now.After(record.UpdatedAt.Add(DefaultLeaseDuration))
}

func DeriveDownstreamKey(rootKey string, stepID string) string {
	defer observability.TraceEnter(nil)()
	combined := fmt.Sprintf("%s:%s", rootKey, stepID)
	sum := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(sum[:])
}

package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
)

func EvaluateExistingRecord(
	existing *contracts.IdempotencyRecord,
	incomingFingerprint string,
	now time.Time,
) contracts.AcquireOutcome {
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
		if existing.LeaseUntil != nil && now.After(*existing.LeaseUntil) {
			return contracts.OutcomeAcquired
		}
		return contracts.OutcomeConflictInProgress

	case contracts.StateUnknown:
		return contracts.OutcomeConflictInProgress

	default:
		return contracts.OutcomeConflictInProgress
	}
}

func DeriveDownstreamKey(rootKey string, stepID string) string {
	combined := fmt.Sprintf("%s:%s", rootKey, stepID)
	sum := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(sum[:])
}


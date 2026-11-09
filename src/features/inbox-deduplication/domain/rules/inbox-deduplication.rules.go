package rules

import (
	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
)

func EvaluateDuplicateContent(storedHash string, incomingHash string) contracts.IngestOutcome {
	if storedHash == incomingHash {
		return contracts.OutcomeDuplicate
	}
	return contracts.OutcomeIntegrityMismatch
}

func IsSafeDuplicate(storedHash string, incomingHash string) bool {
	return storedHash == incomingHash
}

func IsIntegrityMismatch(storedHash string, incomingHash string) bool {
	return storedHash != incomingHash
}

func ShouldQuarantineOnAttemptLimit(attempts int, maxAttempts int) bool {
	if maxAttempts <= 0 {
		return false
	}
	return attempts >= maxAttempts
}


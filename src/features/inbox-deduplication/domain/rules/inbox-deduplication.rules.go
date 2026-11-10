package rules

import (
	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	"distributed-transaction-engine/src/infra/observability"
)

func EvaluateDuplicateContent(storedHash string, incomingHash string) contracts.IngestOutcome {
	defer observability.TraceEnter(nil)()
	if storedHash == incomingHash {
		return contracts.OutcomeDuplicate
	}
	return contracts.OutcomeIntegrityMismatch
}

func IsSafeDuplicate(storedHash string, incomingHash string) bool {
	defer observability.TraceEnter(nil)()
	return storedHash == incomingHash
}

func IsIntegrityMismatch(storedHash string, incomingHash string) bool {
	defer observability.TraceEnter(nil)()
	return storedHash != incomingHash
}

func ShouldQuarantineOnAttemptLimit(attempts int, maxAttempts int) bool {
	defer observability.TraceEnter(nil)()
	if maxAttempts <= 0 {
		return false
	}
	return attempts >= maxAttempts
}

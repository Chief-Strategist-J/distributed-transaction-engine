package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	rules "distributed-transaction-engine/src/features/inbox-deduplication/domain/rules"
)

func TestRules_EvaluateDuplicateContent(t *testing.T) {
	hashA := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	hashB := "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"

	if outcome := rules.EvaluateDuplicateContent(hashA, hashA); outcome != contracts.OutcomeDuplicate {
		t.Fatalf("expected OutcomeDuplicate, got %v", outcome)
	}

	if outcome := rules.EvaluateDuplicateContent(hashA, hashB); outcome != contracts.OutcomeIntegrityMismatch {
		t.Fatalf("expected OutcomeIntegrityMismatch, got %v", outcome)
	}

	if !rules.IsSafeDuplicate(hashA, hashA) {
		t.Fatalf("expected IsSafeDuplicate to be true")
	}

	if !rules.IsIntegrityMismatch(hashA, hashB) {
		t.Fatalf("expected IsIntegrityMismatch to be true")
	}
}

func TestRules_ShouldQuarantineOnAttemptLimit(t *testing.T) {
	if rules.ShouldQuarantineOnAttemptLimit(2, 5) {
		t.Fatalf("expected false for 2 attempts with limit 5")
	}
	if !rules.ShouldQuarantineOnAttemptLimit(5, 5) {
		t.Fatalf("expected true for 5 attempts with limit 5")
	}
	if !rules.ShouldQuarantineOnAttemptLimit(6, 5) {
		t.Fatalf("expected true for 6 attempts with limit 5")
	}
	if rules.ShouldQuarantineOnAttemptLimit(10, 0) {
		t.Fatalf("expected false for 0 max limit")
	}
}

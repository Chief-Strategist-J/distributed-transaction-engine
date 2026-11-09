package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/features/idempotency-keys/domain/rules"
)

func TestCanonicalFingerprint_KeyOrderIndependence(t *testing.T) {
	payloadA := map[string]interface{}{
		"b": "value_b",
		"a": "value_a",
		"nested": map[string]interface{}{
			"z": 100,
			"y": 50,
		},
	}

	payloadB := map[string]interface{}{
		"a": "value_a",
		"nested": map[string]interface{}{
			"y": 50,
			"z": 100,
		},
		"b": "value_b",
	}

	hashA, err := contracts.ComputeCanonicalFingerprint("orders.create", payloadA)
	if err != nil {
		t.Fatalf("unexpected error hashing payloadA: %v", err)
	}

	hashB, err := contracts.ComputeCanonicalFingerprint("orders.create", payloadB)
	if err != nil {
		t.Fatalf("unexpected error hashing payloadB: %v", err)
	}

	if hashA != hashB {
		t.Fatalf("expected identical hashes regardless of key order, got %s != %s", hashA, hashB)
	}
}

func TestCanonicalFingerprint_TamperDetection(t *testing.T) {
	payloadA := map[string]interface{}{
		"amount": 250.00,
	}
	payloadB := map[string]interface{}{
		"amount": 999.00,
	}

	hashA, _ := contracts.ComputeCanonicalFingerprint("payments.charge", payloadA)
	hashB, _ := contracts.ComputeCanonicalFingerprint("payments.charge", payloadB)

	if hashA == hashB {
		t.Fatalf("expected different hashes for different payloads, got identical %s", hashA)
	}
}

func TestRules_EvaluateExistingRecord(t *testing.T) {
	now := time.Now().UTC()
	fingerprint := "hash_abc_123"

	outcome := rules.EvaluateExistingRecord(nil, fingerprint, now)
	if outcome != contracts.OutcomeAcquired {
		t.Fatalf("expected ACQUIRED for nil record, got %s", outcome)
	}

	completedRec := &contracts.IdempotencyRecord{
		RequestFingerprint: fingerprint,
		CurrentState:       contracts.StateCompleted,
	}
	outcome = rules.EvaluateExistingRecord(completedRec, fingerprint, now)
	if outcome != contracts.OutcomeReplay {
		t.Fatalf("expected REPLAY for completed record with matching fingerprint, got %s", outcome)
	}

	outcome = rules.EvaluateExistingRecord(completedRec, "hash_tampered", now)
	if outcome != contracts.OutcomeFingerprintMismatch {
		t.Fatalf("expected FINGERPRINT_MISMATCH for completed record with tampered fingerprint, got %s", outcome)
	}

	activeLease := now.Add(10 * time.Second)
	inProgressRec := &contracts.IdempotencyRecord{
		RequestFingerprint: fingerprint,
		CurrentState:       contracts.StateInProgress,
		LeaseUntil:         &activeLease,
	}
	outcome = rules.EvaluateExistingRecord(inProgressRec, fingerprint, now)
	if outcome != contracts.OutcomeConflictInProgress {
		t.Fatalf("expected CONFLICT_IN_PROGRESS for active in_progress record, got %s", outcome)
	}

	expiredLease := now.Add(-10 * time.Second)
	expiredRec := &contracts.IdempotencyRecord{
		RequestFingerprint: fingerprint,
		CurrentState:       contracts.StateInProgress,
		LeaseUntil:         &expiredLease,
	}
	outcome = rules.EvaluateExistingRecord(expiredRec, fingerprint, now)
	if outcome != contracts.OutcomeAcquired {
		t.Fatalf("expected ACQUIRED for expired lease record, got %s", outcome)
	}
}

func TestRules_DeriveDownstreamKey(t *testing.T) {
	rootKey := "ik_root_test_1001"
	stepID := "step_payment_reserve"

	key1 := rules.DeriveDownstreamKey(rootKey, stepID)
	key2 := rules.DeriveDownstreamKey(rootKey, stepID)

	if key1 != key2 {
		t.Fatalf("expected derived key to be deterministic, got %s != %s", key1, key2)
	}

	keyOtherStep := rules.DeriveDownstreamKey(rootKey, "step_notify")
	if key1 == keyOtherStep {
		t.Fatalf("expected different steps to produce different derived keys, got identical %s", key1)
	}
}


package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/domain/rules"
)

func TestRules_ComputeAndVerifyBundleHash(t *testing.T) {
	payload := map[string]interface{}{
		"workflow": "order-fulfillment",
		"steps":    []interface{}{"reserve_stock", "charge_card"},
	}
	hash, err := rules.ComputeBundleHash(payload)
	if err != nil {
		t.Fatalf("unexpected error computing hash: %v", err)
	}
	if hash == "" {
		t.Fatal("expected non-empty hash")
	}
	if err := rules.VerifyBundleIntegrity(payload, hash); err != nil {
		t.Fatalf("expected integrity match, got: %v", err)
	}
	tampered := map[string]interface{}{
		"workflow": "order-fulfillment",
		"steps":    []interface{}{"reserve_stock"},
	}
	if err := rules.VerifyBundleIntegrity(tampered, hash); err == nil {
		t.Fatal("expected integrity failure for tampered payload")
	}
}

func TestRules_MakerCheckerValidation(t *testing.T) {
	err := rules.ValidateMakerChecker("alice", "alice")
	if err == nil {
		t.Fatal("expected error when proposer equals approver")
	}
	if err := rules.ValidateMakerChecker("alice", "bob"); err != nil {
		t.Fatalf("expected success for distinct users, got: %v", err)
	}
	if err := rules.ValidateMakerChecker("", "bob"); err == nil {
		t.Fatal("expected validation error for empty proposer")
	}
}

func TestRules_MatchWorkerCapabilities(t *testing.T) {
	required := []string{"gpu", "wasm"}
	if !rules.MatchWorkerCapabilities(required, []string{"gpu", "wasm", "high-mem"}) {
		t.Fatal("expected capabilities to match")
	}
	if rules.MatchWorkerCapabilities(required, []string{"gpu"}) {
		t.Fatal("expected capabilities mismatch when missing required tag")
	}
}

func TestRules_CohortDeterminism(t *testing.T) {
	b1 := rules.ComputeEntityCohortBucket("customer_12345")
	b2 := rules.ComputeEntityCohortBucket("customer_12345")
	if b1 != b2 {
		t.Fatalf("expected deterministic cohort bucket, got %d and %d", b1, b2)
	}
	if b1 < 0 || b1 >= 100 {
		t.Fatalf("cohort bucket out of bounds: %d", b1)
	}
}

func TestRules_SnapshotLiveInputs(t *testing.T) {
	orig := map[string]interface{}{
		"feature_flag_discount": true,
		"rate_limit":            100,
	}
	snap := rules.SnapshotLiveInputs(orig)
	orig["rate_limit"] = 200
	if snap["rate_limit"] == 200 {
		t.Fatal("expected snapshot to be decoupled from original mutation")
	}
}

func TestRules_CanMigrateAtSafePoint(t *testing.T) {
	if err := rules.CanMigrateAtSafePoint("step_payment", "step_payment", contracts.BundleStatusActive); err != nil {
		t.Fatalf("expected safe point migration to pass, got: %v", err)
	}
	if err := rules.CanMigrateAtSafePoint("step_payment", "step_payment", contracts.BundleStatusDraft); err == nil {
		t.Fatal("expected error migrating to non-ACTIVE bundle")
	}
	if err := rules.CanMigrateAtSafePoint("step_shipping", "step_payment", contracts.BundleStatusActive); err == nil {
		t.Fatal("expected conflict error when step does not match safe point")
	}
}


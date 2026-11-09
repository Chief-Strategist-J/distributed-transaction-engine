package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	rules "distributed-transaction-engine/src/features/sequential-execution/domain/rules"
)

func TestNextForwardStepIndex(t *testing.T) {
	idx, ok := rules.NextForwardStepIndex(0, 3)
	if !ok || idx != 1 {
		t.Fatalf("expected next index 1, got %d (ok: %v)", idx, ok)
	}

	idx, ok = rules.NextForwardStepIndex(2, 3)
	if ok {
		t.Fatalf("expected hasMore to be false at boundary, got %v with index %d", ok, idx)
	}
}

func TestNextCompensatingStepIndex(t *testing.T) {
	idx, ok := rules.NextCompensatingStepIndex(2)
	if !ok || idx != 1 {
		t.Fatalf("expected next compensation index 1, got %d (ok: %v)", idx, ok)
	}

	idx, ok = rules.NextCompensatingStepIndex(0)
	if ok {
		t.Fatalf("expected hasMore false at 0, got %v with index %d", ok, idx)
	}
}

func TestValidateStepOrdering(t *testing.T) {
	valid := []contracts.StepNature{
		contracts.NatureCompensable,
		contracts.NatureCompensable,
		contracts.NaturePivot,
		contracts.NatureRetriable,
	}
	if err := rules.ValidateStepOrdering(valid); err != nil {
		t.Fatalf("expected valid ordering, got: %v", err)
	}

	invalid := []contracts.StepNature{
		contracts.NaturePivot,
		contracts.NatureCompensable,
	}
	if err := rules.ValidateStepOrdering(invalid); err == nil {
		t.Fatal("expected error when compensable follows pivot, got nil")
	}
}

func TestCanCompensate(t *testing.T) {
	if rules.CanCompensate(contracts.PhaseForward, true, contracts.NatureCompensable) {
		t.Fatal("cannot compensate once pivot is reached")
	}
	if !rules.CanCompensate(contracts.PhaseForward, false, contracts.NatureCompensable) {
		t.Fatal("must be able to compensate compensable step before pivot")
	}
}

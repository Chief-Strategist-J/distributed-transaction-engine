package tests

import (
	"testing"

	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
)

func TestRules_FindInitialExecutableSteps(t *testing.T) {
	steps := []workflowContracts.StepDefinition{
		{
			StepID:       "reserve_flight",
			Dependencies: []string{},
		},
		{
			StepID:       "reserve_hotel",
			Dependencies: []string{},
		},
		{
			StepID:       "charge_card",
			Dependencies: []string{"reserve_flight", "reserve_hotel"},
		},
	}

	roots := sagaRules.FindInitialExecutableSteps(steps)
	if len(roots) != 2 {
		t.Fatalf("expected 2 root steps, got %d", len(roots))
	}
	if roots[0] != "reserve_flight" || roots[1] != "reserve_hotel" {
		t.Fatalf("unexpected root steps: %v", roots)
	}
}

func TestRules_FindNextExecutableSteps(t *testing.T) {
	steps := []workflowContracts.StepDefinition{
		{
			StepID:       "step_a",
			Dependencies: []string{},
		},
		{
			StepID:       "step_b",
			Dependencies: []string{},
		},
		{
			StepID:       "step_c",
			Dependencies: []string{"step_a", "step_b"},
		},
		{
			StepID:       "step_d",
			Dependencies: []string{"step_c"},
		},
	}

	completed := map[string]bool{"step_a": true}
	inFlight := map[string]bool{"step_b": true}

	next := sagaRules.FindNextExecutableSteps(steps, completed, inFlight)
	if len(next) != 0 {
		t.Fatalf("expected 0 ready steps since step_b is not yet completed, got %v", next)
	}

	completed["step_b"] = true
	delete(inFlight, "step_b")

	next = sagaRules.FindNextExecutableSteps(steps, completed, inFlight)
	if len(next) != 1 || next[0] != "step_c" {
		t.Fatalf("expected step_c to become ready, got %v", next)
	}
}

func TestRules_ComputeReverseCompensationChain(t *testing.T) {
	steps := []workflowContracts.StepDefinition{
		{
			StepID:             "reserve_inventory",
			IsPivot:            false,
			CompensationAction: "RELEASE_INVENTORY",
		},
		{
			StepID:             "reserve_shipping",
			IsPivot:            false,
			CompensationAction: "CANCEL_SHIPPING",
		},
		{
			StepID:             "charge_payment",
			IsPivot:            true,
			CompensationAction: "",
		},
	}

	topoOrder := []string{"reserve_inventory", "reserve_shipping", "charge_payment"}
	succeeded := map[string]bool{
		"reserve_inventory": true,
		"reserve_shipping":  true,
	}

	compChain := sagaRules.ComputeReverseCompensationChain(steps, topoOrder, succeeded)
	if len(compChain) != 2 {
		t.Fatalf("expected 2 steps to compensate, got %d", len(compChain))
	}
	if compChain[0] != "reserve_shipping" || compChain[1] != "reserve_inventory" {
		t.Fatalf("expected reverse topological order [reserve_shipping, reserve_inventory], got %v", compChain)
	}
}


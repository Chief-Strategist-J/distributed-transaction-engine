package tests

import (
	"strings"
	"testing"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	"distributed-transaction-engine/src/shared/errors"
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

func TestRules_DetermineFailureRouting(t *testing.T) {
	if route := sagaRules.DetermineFailureRouting(true); route != "FORWARD_ONLY" {
		t.Fatalf("expected post-pivot route to be FORWARD_ONLY, got %s", route)
	}
	if route := sagaRules.DetermineFailureRouting(false); route != "BACKWARD" {
		t.Fatalf("expected pre-pivot route to be BACKWARD, got %s", route)
	}
}

func TestRules_IdempotencyKeySuffixes(t *testing.T) {
	fwd := sagaRules.DeriveForwardIdempotencyKey("sga_1", "step_a")
	comp := sagaRules.DeriveCompensationIdempotencyKey("sga_1", "step_a")

	if !strings.HasSuffix(fwd, ":fwd") {
		t.Fatalf("expected forward key to end with :fwd, got: %s", fwd)
	}
	if !strings.HasSuffix(comp, ":comp") {
		t.Fatalf("expected compensation key to end with :comp, got: %s", comp)
	}
	if fwd == comp {
		t.Fatalf("forward and compensation keys must not be equal")
	}
}

func TestRules_ComputeReverseCompensationChainWithUnknownStep(t *testing.T) {
	steps := []workflowContracts.StepDefinition{
		{
			StepID:             "step_1",
			IsPivot:            false,
			CompensationAction: "UNDO_1",
		},
		{
			StepID:             "step_2",
			IsPivot:            false,
			CompensationAction: "UNDO_2",
		},
	}

	topoOrder := []string{"step_1", "step_2"}
	eligible := map[string]bool{
		"step_1": true,
		"step_2": true,
	}

	chain := sagaRules.ComputeReverseCompensationChain(steps, topoOrder, eligible)
	if len(chain) != 2 || chain[0] != "step_2" || chain[1] != "step_1" {
		t.Fatalf("expected [step_2, step_1], got %v", chain)
	}
}

func TestRules_ClassifyStepResult(t *testing.T) {
	succ := sagaRules.ClassifyStepResult(false, nil)
	if succ != sagaContracts.StepStateSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", succ)
	}

	ambig := sagaRules.ClassifyStepResult(true, errors.NewInternalError("timeout"))
	if ambig != sagaContracts.StepStateUnknown {
		t.Fatalf("expected UNKNOWN for ambiguous error, got %s", ambig)
	}

	fail := sagaRules.ClassifyStepResult(false, errors.NewValidationError("400 bad request"))
	if fail != sagaContracts.StepStateFailed {
		t.Fatalf("expected FAILED for definitive error, got %s", fail)
	}
}

func TestRules_DeriveDrainPlan(t *testing.T) {
	steps := []sagaContracts.SagaStepExecution{
		{
			StepID:         "step_pending",
			ExecutionState: sagaContracts.StepStatePending,
		},
		{
			StepID:         "step_dispatched_in_flight",
			ExecutionState: sagaContracts.StepStateDispatched,
		},
		{
			StepID:         "step_succeeded",
			ExecutionState: sagaContracts.StepStateSucceeded,
		},
	}

	inFlight := map[string]bool{"step_dispatched_in_flight": true}

	drained, compSet := sagaRules.DeriveDrainPlan(steps, inFlight)
	if len(drained) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(drained))
	}
	if drained[0].ExecutionState != sagaContracts.StepStateNotStarted {
		t.Fatalf("expected PENDING to become NOT_STARTED, got %s", drained[0].ExecutionState)
	}
	if drained[1].ExecutionState != sagaContracts.StepStateUnknown {
		t.Fatalf("expected in-flight DISPATCHED to become UNKNOWN, got %s", drained[1].ExecutionState)
	}
	if !compSet["step_dispatched_in_flight"] || !compSet["step_succeeded"] {
		t.Fatalf("expected step_dispatched_in_flight and step_succeeded in compensationSet, got %v", compSet)
	}
	if compSet["step_pending"] {
		t.Fatalf("step_pending must NOT be in compensationSet")
	}
}

func TestRules_EvaluatePostCompensationState(t *testing.T) {
	if s := sagaRules.EvaluatePostCompensationState(true); s != sagaContracts.SagaStateFailedCompensated {
		t.Fatalf("expected FAILED_COMPENSATED, got %s", s)
	}
	if s := sagaRules.EvaluatePostCompensationState(false); s != sagaContracts.SagaStateCompensationStuck {
		t.Fatalf("expected COMPENSATION_STUCK, got %s", s)
	}
}

func TestRules_ValidateSagaTransition(t *testing.T) {
	if err := sagaRules.ValidateSagaTransition(sagaContracts.SagaStateCreated, sagaContracts.SagaStateRunning); err != nil {
		t.Fatalf("expected valid transition from CREATED to RUNNING, got %v", err)
	}
	if err := sagaRules.ValidateSagaTransition(sagaContracts.SagaStateRunning, sagaContracts.SagaStateCompleted); err != nil {
		t.Fatalf("expected valid transition from RUNNING to COMPLETED, got %v", err)
	}
	if err := sagaRules.ValidateSagaTransition(sagaContracts.SagaStateCompleted, sagaContracts.SagaStateRunning); err == nil {
		t.Fatalf("expected error on transition from terminal COMPLETED to RUNNING")
	}
}

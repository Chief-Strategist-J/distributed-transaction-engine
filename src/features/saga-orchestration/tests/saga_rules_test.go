package tests

import (
	"testing"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
)

func TestCanTransition(t *testing.T) {
	if !sagaRules.CanTransition(sagaContracts.SagaStatePending, sagaContracts.SagaStateRunning) {
		t.Errorf("expected transition to be valid")
	}
	if sagaRules.CanTransition(sagaContracts.SagaStatePending, sagaContracts.SagaStateCompleted) {
		t.Errorf("expected transition to be invalid")
	}
}

func TestRules_IsProcessableState(t *testing.T) {
	cases := map[sagaContracts.SagaState]bool{
		sagaContracts.SagaStateCreated: true, sagaContracts.SagaStateRunning: true, sagaContracts.SagaStateRollingForward: true,
		sagaContracts.SagaStateCompensating: true, sagaContracts.SagaStateCompleted: false, sagaContracts.SagaStateFailed: false,
		sagaContracts.SagaStateFailedCompensated: false, sagaContracts.SagaStateCancelled: false, sagaContracts.SagaStateCompensationStuck: false,
		sagaContracts.SagaStateStuckForward: false, "": false,
	}
	for state, want := range cases {
		if got := sagaRules.IsProcessableState(state); got != want {
			t.Fatalf("state %q expected processable=%v got %v", state, want, got)
		}
	}
}

func TestRules_DeriveResumeCompensationSet(t *testing.T) {
	steps := []sagaContracts.SagaStepExecution{
		{StepID: "done", ExecutionState: sagaContracts.StepStateSucceeded},
		{StepID: "ambiguous", ExecutionState: sagaContracts.StepStateUnknown},
		{StepID: "interrupted", ExecutionState: sagaContracts.StepStateCompensating},
		{StepID: "already", ExecutionState: sagaContracts.StepStateCompensated},
		{StepID: "never", ExecutionState: sagaContracts.StepStateNotStarted},
		{StepID: "failed", ExecutionState: sagaContracts.StepStateFailed},
		{StepID: "pending", ExecutionState: sagaContracts.StepStatePending},
	}
	eligible := sagaRules.DeriveResumeCompensationSet(steps)
	for step, want := range map[string]bool{"done": true, "ambiguous": true, "interrupted": true, "already": false, "never": false, "failed": false, "pending": false} {
		if eligible[step] != want {
			t.Fatalf("step %s expected eligible=%v", step, want)
		}
	}
}

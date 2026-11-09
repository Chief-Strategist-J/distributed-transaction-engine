package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	machines "distributed-transaction-engine/src/features/sequential-execution/domain/machines"
)

func TestSequentialExecutionMachine_ForwardSuccess(t *testing.T) {
	m := machines.NewSequentialExecutionMachine()

	dec, err := m.EvaluateStepSuccess(contracts.PhaseForward, 0, 3, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.NextStepIndex != 1 || dec.NextSagaState != "RUNNING" {
		t.Fatalf("expected step 1 RUNNING, got %d %s", dec.NextStepIndex, dec.NextSagaState)
	}
	if len(dec.Commands) != 1 || dec.Commands[0].Type != machines.CommandDispatchStep {
		t.Fatalf("expected dispatch command, got: %v", dec.Commands)
	}

	decFinal, err := m.EvaluateStepSuccess(contracts.PhaseForward, 2, 3, false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decFinal.NextSagaState != "COMPLETED" {
		t.Fatalf("expected COMPLETED, got %s", decFinal.NextSagaState)
	}
}

func TestSequentialExecutionMachine_PrePivotFailureTriggersCompensation(t *testing.T) {
	m := machines.NewSequentialExecutionMachine()

	dec, err := m.EvaluateStepFailure(contracts.PhaseForward, 2, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.NextPhase != contracts.PhaseCompensating || dec.NextStepIndex != 1 {
		t.Fatalf("expected PhaseCompensating index 1, got %s %d", dec.NextPhase, dec.NextStepIndex)
	}
	if dec.Commands[0].Type != machines.CommandCompensateStep {
		t.Fatalf("expected CommandCompensateStep, got %v", dec.Commands[0].Type)
	}
}

func TestSequentialExecutionMachine_PostPivotFailureRollsForward(t *testing.T) {
	m := machines.NewSequentialExecutionMachine()

	dec, err := m.EvaluateStepFailure(contracts.PhaseForward, 2, true, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.NextPhase != contracts.PhaseRollForward {
		t.Fatalf("expected PhaseRollForward, got: %s", dec.NextPhase)
	}
	if dec.Commands[0].Type != machines.CommandEscalateAttention {
		t.Fatalf("expected escalate command, got: %v", dec.Commands[0].Type)
	}
}

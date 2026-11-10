package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/machines"
)

func TestScheduledExecutionMachineValidTransitions(t *testing.T) {
	m := machines.NewScheduledExecutionMachine()

	to, err := m.Transition(contracts.ScheduleActive, contracts.SchedulePaused)
	if err != nil || to != contracts.SchedulePaused {
		t.Fatalf("expected valid transition from ACTIVE to PAUSED")
	}

	to, err = m.Transition(contracts.SchedulePaused, contracts.ScheduleActive)
	if err != nil || to != contracts.ScheduleActive {
		t.Fatalf("expected valid transition from PAUSED to ACTIVE")
	}
}

func TestScheduledExecutionMachineTerminalTransition(t *testing.T) {
	m := machines.NewScheduledExecutionMachine()

	_, err := m.Transition(contracts.ScheduleCompleted, contracts.ScheduleActive)
	if err == nil {
		t.Fatalf("expected error on transition from terminal COMPLETED state")
	}
}

package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/machines"
)

func TestReconciliationMachineValidTransitions(t *testing.T) {
	m := machines.NewReconciliationMachine()

	to, err := m.Transition(contracts.StatusAnomalyDetected, contracts.StatusRepairScheduled)
	if err != nil || to != contracts.StatusRepairScheduled {
		t.Fatalf("expected valid transition to REPAIR_SCHEDULED")
	}

	to, err = m.Transition(contracts.StatusRepairScheduled, contracts.StatusAutoResolved)
	if err != nil || to != contracts.StatusAutoResolved {
		t.Fatalf("expected valid transition to AUTO_RESOLVED")
	}
}

func TestReconciliationMachineInvalidTransitions(t *testing.T) {
	m := machines.NewReconciliationMachine()

	_, err := m.Transition(contracts.StatusAutoResolved, contracts.StatusAnomalyDetected)
	if err == nil {
		t.Fatalf("expected error on transition from terminal state")
	}
}

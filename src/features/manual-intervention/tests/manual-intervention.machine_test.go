package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/features/manual-intervention/domain/machines"
)

func TestManualInterventionMachineEligibility(t *testing.T) {
	m := machines.NewManualInterventionMachine()

	if err := m.ValidateActionEligibility(contracts.ActionForceRetry, "STUCK"); err != nil {
		t.Fatalf("expected STUCK to be eligible for FORCE_RETRY")
	}

	if err := m.ValidateActionEligibility(contracts.ActionForceComplete, "RUNNING"); err == nil {
		t.Fatalf("expected RUNNING to be ineligible for FORCE_COMPLETE")
	}
}

package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	machines "distributed-transaction-engine/src/features/transaction-query-api/domain/machines"
)

func TestExportStatusMachine_Transitions(t *testing.T) {
	allowed := machines.IsExportTransitionAllowed(contracts.ExportQueued, contracts.ExportRunning)
	if !allowed {
		t.Fatal("expected ExportQueued -> ExportRunning to be allowed")
	}

	next, err := machines.TransitionToExportStatus(contracts.ExportQueued, contracts.ExportRunning)
	if err != nil || next != contracts.ExportRunning {
		t.Fatalf("expected transition to ExportRunning, got %s err: %v", next, err)
	}

	next, err = machines.TransitionToExportStatus(contracts.ExportRunning, contracts.ExportCompleted)
	if err != nil || next != contracts.ExportCompleted {
		t.Fatalf("expected transition to ExportCompleted, got %s err: %v", next, err)
	}

	_, err = machines.TransitionToExportStatus(contracts.ExportCompleted, contracts.ExportRunning)
	if err == nil {
		t.Fatal("expected conflict error for invalid transition ExportCompleted -> ExportRunning")
	}
}


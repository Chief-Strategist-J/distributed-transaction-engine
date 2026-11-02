package tests

import (
	"context"
	"testing"

	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
)

func TestEndToEndLifecycleFromCreationToTerminalAudit(t *testing.T) {
	ctx := context.Background()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)

	tenantID := "tenant_e2e"
	sagaID := "018d9f1c-7201-7000-8000-000000000501"
	publicID := "sag_55555555555555555555aaaaaa"
	source := "e2e-suite"
	externalID := "e2e_tx_001"
	correlationID := "018d9f1c-7201-7000-8000-000000000599"

	initialRecord, err := svc.InitializeState(ctx, contracts.InitializeStateInput{
		ID:            sagaID,
		PublicID:      publicID,
		TenantID:      tenantID,
		Source:        source,
		ExternalID:    externalID,
		CorrelationID: correlationID,
	})
	if err != nil {
		t.Fatalf("e2e init failed: %v", err)
	}

	stateRecord, err := svc.GetState(ctx, tenantID, sagaID)
	if err != nil || stateRecord.CurrentState != contracts.StateCreated {
		t.Fatalf("e2e get state mismatch: %+v", stateRecord)
	}

	stateRecord, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventOrchestratorClaim,
		ExpectedVersion: stateRecord.Version,
		FencingToken:    10,
		Reason:          "worker claimed lease",
	})
	if err != nil || stateRecord.CurrentState != contracts.StateRunning {
		t.Fatalf("e2e transition to RUNNING failed: %v", err)
	}

	stateRecord, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventAsyncSuspend,
		ExpectedVersion: stateRecord.Version,
		FencingToken:    10,
		Reason:          "suspending for external verification",
	})
	if err != nil || stateRecord.CurrentState != contracts.StateWaiting {
		t.Fatalf("e2e transition to WAITING failed: %v", err)
	}

	stateRecord, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventWebhookApproved,
		ExpectedVersion: stateRecord.Version,
		FencingToken:    11,
		Reason:          "webhook verification approved",
	})
	if err != nil || stateRecord.CurrentState != contracts.StateRunning {
		t.Fatalf("e2e resume to RUNNING failed: %v", err)
	}

	stateRecord, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventDAGCompleted,
		ExpectedVersion: stateRecord.Version,
		FencingToken:    11,
		Reason:          "all workflow steps completed",
	})
	if err != nil || stateRecord.CurrentState != contracts.StateCompleted {
		t.Fatalf("e2e transition to COMPLETED failed: %v", err)
	}

	finalRecord, err := svc.GetState(ctx, tenantID, sagaID)
	if err != nil || finalRecord.CurrentState != contracts.StateCompleted || finalRecord.Version != 5 {
		t.Fatalf("final state record assertion failed: %+v", finalRecord)
	}

	auditLogs, err := svc.GetAuditLogs(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("failed to retrieve audit logs: %v", err)
	}
	if len(auditLogs) != 4 {
		t.Fatalf("expected 4 transition audit records, got %d", len(auditLogs))
	}

	expectedTransitions := []struct {
		from contracts.TransactionState
		to   contracts.TransactionState
	}{
		{contracts.StateCreated, contracts.StateRunning},
		{contracts.StateRunning, contracts.StateWaiting},
		{contracts.StateWaiting, contracts.StateRunning},
		{contracts.StateRunning, contracts.StateCompleted},
	}

	for idx, expected := range expectedTransitions {
		if auditLogs[idx].FromState != expected.from || auditLogs[idx].ToState != expected.to {
			t.Errorf("audit log %d mismatch: expected %s -> %s, got %s -> %s",
				idx, expected.from, expected.to, auditLogs[idx].FromState, auditLogs[idx].ToState)
		}
		if auditLogs[idx].CorrelationID != correlationID {
			t.Errorf("audit log %d missing correlation ID: %s", idx, auditLogs[idx].CorrelationID)
		}
	}

	if initialRecord.ID != finalRecord.ID || initialRecord.TenantID != finalRecord.TenantID {
		t.Fatalf("immutability of surrogate id or tenant id breached")
	}
}

package tests

import (
	"context"
	"testing"

	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
)

func TestIntegrationAdapterLifecycleAndAuditPersistence(t *testing.T) {
	ctx := context.Background()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)

	tenantID := "tenant_integration"
	sagaID := "018d9f1c-7201-7000-8000-000000000101"
	publicID := "sag_11111111111111111111aaaaaa"
	source := "integration-test-runner"
	externalID := "int_tx_001"
	correlationID := "018d9f1c-7201-7000-8000-000000000199"

	_, err := svc.InitializeState(ctx, contracts.InitializeStateInput{
		ID:            sagaID,
		PublicID:      publicID,
		TenantID:      tenantID,
		Source:        source,
		ExternalID:    externalID,
		CorrelationID: correlationID,
	})
	if err != nil {
		t.Fatalf("failed to initialize state in adapter: %v", err)
	}

	fetchedRecord, err := svc.GetState(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("failed to query state from adapter: %v", err)
	}
	if fetchedRecord.CurrentState != contracts.StateCreated || fetchedRecord.Version != 1 {
		t.Fatalf("state record mismatch: %+v", fetchedRecord)
	}

	runningRecord, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventOrchestratorClaim,
		ExpectedVersion: 1,
		FencingToken:    50,
		Reason:          "worker claimed execution lease",
	})
	if err != nil {
		t.Fatalf("failed to transition to RUNNING: %v", err)
	}
	if runningRecord.CurrentState != contracts.StateRunning || runningRecord.Version != 2 {
		t.Fatalf("unexpected running record: %+v", runningRecord)
	}

	audits, err := svc.GetAuditLogs(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("failed to query audit logs from adapter: %v", err)
	}
	if len(audits) != 1 {
		t.Fatalf("expected 1 audit log entry, got %d", len(audits))
	}
	if audits[0].FromState != contracts.StateCreated || audits[0].ToState != contracts.StateRunning {
		t.Fatalf("unexpected audit transition states: %+v", audits[0])
	}
	if audits[0].CorrelationID != correlationID {
		t.Fatalf("expected audit correlationId %s, got %s", correlationID, audits[0].CorrelationID)
	}
}

func TestIntegrationOptimisticConcurrencyRace(t *testing.T) {
	ctx := context.Background()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)

	tenantID := "tenant_concurrency"
	sagaID := "018d9f1c-7201-7000-8000-000000000102"
	publicID := "sag_11111111111111111111bbbbbb"
	source := "concurrency-runner"
	externalID := "int_tx_002"
	correlationID := "018d9f1c-7201-7000-8000-000000000198"

	_, err := svc.InitializeState(ctx, contracts.InitializeStateInput{
		ID:            sagaID,
		PublicID:      publicID,
		TenantID:      tenantID,
		Source:        source,
		ExternalID:    externalID,
		CorrelationID: correlationID,
	})
	if err != nil {
		t.Fatalf("failed to initialize: %v", err)
	}

	_, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventOrchestratorClaim,
		ExpectedVersion: 1,
		FencingToken:    10,
		Reason:          "worker 1 claimed",
	})
	if err != nil {
		t.Fatalf("worker 1 claim failed: %v", err)
	}

	_, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventClientAbort,
		ExpectedVersion: 1,
		FencingToken:    10,
		Reason:          "worker 2 late cancel attempt with stale version 1",
	})
	if err == nil {
		t.Fatalf("expected conflict error when worker 2 attempts transition on stale version")
	}
}

func TestIntegrationFencingTokenSplitBrainRejection(t *testing.T) {
	ctx := context.Background()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)

	tenantID := "tenant_fencing"
	sagaID := "018d9f1c-7201-7000-8000-000000000103"
	publicID := "sag_11111111111111111111cccccc"
	source := "fencing-runner"
	externalID := "int_tx_003"
	correlationID := "018d9f1c-7201-7000-8000-000000000197"

	_, _ = svc.InitializeState(ctx, contracts.InitializeStateInput{
		ID:            sagaID,
		PublicID:      publicID,
		TenantID:      tenantID,
		Source:        source,
		ExternalID:    externalID,
		CorrelationID: correlationID,
	})

	_, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventOrchestratorClaim,
		ExpectedVersion: 1,
		FencingToken:    100,
		Reason:          "worker B acquired lease with fencing token 100",
	})
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	_, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventStepSuccess,
		ExpectedVersion: 2,
		FencingToken:    42,
		Reason:          "stale worker A attempts commit with zombie token 42",
	})
	if err == nil {
		t.Fatalf("expected rejection of stale fencing token 42 against active token 100")
	}
}

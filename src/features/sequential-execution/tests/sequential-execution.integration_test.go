package tests

import (
	"context"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	machines "distributed-transaction-engine/src/features/sequential-execution/domain/machines"
	adapters "distributed-transaction-engine/src/features/sequential-execution/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/sequential-execution/service"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowAdapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	"distributed-transaction-engine/src/infra/observability"
)

func setupTestEnvironment() (*service.SequentialExecutionService, *adapters.MemorySequentialExecutionRepositoryAdapter, *adapters.MemorySequentialDispatcherAdapter, *workflowAdapters.MemoryWorkflowDefinitionsRepositoryAdapter) {
	repo := adapters.NewMemorySequentialExecutionRepositoryAdapter()
	wfRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	dispatcher := adapters.NewMemorySequentialDispatcherAdapter()
	machine := machines.NewSequentialExecutionMachine()
	tracer := observability.NewTracer("test-tracer")

	svc := service.NewSequentialExecutionService(service.SequentialExecutionServiceDeps{
		Repo:         repo,
		WorkflowRepo: wfRepo,
		Dispatcher:   dispatcher,
		Machine:      machine,
		Tracer:       tracer,
	})
	return svc, repo, dispatcher, wfRepo
}

func TestSequentialExecutionService_E2EHappyPath(t *testing.T) {
	svc, _, dispatcher, wfRepo := setupTestEnvironment()
	ctx := context.Background()

	wf := &workflowContracts.WorkflowDefinitionInstance{
		ID:            "88888888-8888-4888-8888-888888888888",
		PublicID:      "wfd_01HXYZ0123456789ABCDEF012",
		TenantID:      "tenant-alpha",
		Name:          "payment-pipeline",
		Version:       1,
		Status:        "ACTIVE",
		CorrelationID: "11111111-1111-4111-8111-111111111111",
		Steps: []workflowContracts.StepDefinition{
			{StepID: "step_auth", StepName: "Authorize", IsPivot: false},
			{StepID: "step_capture", StepName: "Capture", IsPivot: true},
			{StepID: "step_receipt", StepName: "SendReceipt", IsPivot: false},
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	_ = wfRepo.Save(ctx, wf)

	startInput := &contracts.StartSequentialSagaInput{
		ID:                   "22222222-2222-4222-8222-222222222222",
		PublicID:             "seq_01HXYZ0123456789ABCDEF0123",
		TenantID:             "tenant-alpha",
		WorkflowDefinitionID: wf.ID,
		Source:               "CHECKOUT",
		ExternalID:           "order_1001",
		CorrelationID:        "33333333-3333-4333-8333-333333333333",
		Payload:              map[string]interface{}{"amount": 2500},
	}

	state, err := svc.StartSequentialSaga(ctx, startInput)
	if err != nil {
		t.Fatalf("failed to start saga: %v", err)
	}
	if state.CurrentState != "RUNNING" || state.CurrentStepIndex != 0 {
		t.Fatalf("unexpected state after start: %s, idx %d", state.CurrentState, state.CurrentStepIndex)
	}

	if err := svc.HandleStepCompletion(ctx, "tenant-alpha", state.ID, 0, true, map[string]interface{}{"auth_id": "auth_123"}, false); err != nil {
		t.Fatalf("failed step 0: %v", err)
	}

	s1, _ := svc.GetSaga(ctx, "tenant-alpha", state.ID)
	if s1.CurrentStepIndex != 1 {
		t.Fatalf("expected step 1, got %d", s1.CurrentStepIndex)
	}

	if err := svc.HandleStepCompletion(ctx, "tenant-alpha", state.ID, 1, true, map[string]interface{}{"capture_id": "cap_456"}, false); err != nil {
		t.Fatalf("failed step 1: %v", err)
	}

	s2, _ := svc.GetSaga(ctx, "tenant-alpha", state.ID)
	if !s2.PivotReached {
		t.Fatal("expected pivotReached to be true after step 1")
	}

	if err := svc.HandleStepCompletion(ctx, "tenant-alpha", state.ID, 2, true, map[string]interface{}{"status": "done"}, false); err != nil {
		t.Fatalf("failed step 2: %v", err)
	}

	s3, _ := svc.GetSaga(ctx, "tenant-alpha", state.ID)
	if s3.CurrentState != "COMPLETED" {
		t.Fatalf("expected COMPLETED, got %s", s3.CurrentState)
	}

	if len(dispatcher.Dispatched) != 3 {
		t.Fatalf("expected 3 dispatches, got %d", len(dispatcher.Dispatched))
	}
}

func TestSequentialExecutionService_PrePivotFailureTriggersReverseCompensation(t *testing.T) {
	svc, _, dispatcher, wfRepo := setupTestEnvironment()
	ctx := context.Background()

	wf := &workflowContracts.WorkflowDefinitionInstance{
		ID:            "99999999-9999-4999-8999-999999999999",
		PublicID:      "wfd_01HXYZ0123456789ABCDEF099",
		TenantID:      "tenant-alpha",
		Name:          "booking-pipeline",
		Version:       1,
		Status:        "ACTIVE",
		CorrelationID: "11111111-1111-4111-8111-111111111111",
		Steps: []workflowContracts.StepDefinition{
			{StepID: "step_hotel", StepName: "BookHotel", IsPivot: false},
			{StepID: "step_flight", StepName: "BookFlight", IsPivot: false},
			{StepID: "step_car", StepName: "BookCar", IsPivot: true},
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	_ = wfRepo.Save(ctx, wf)

	startInput := &contracts.StartSequentialSagaInput{
		ID:                   "44444444-4444-4444-8444-444444444444",
		PublicID:             "seq_01HXYZ0123456789ABCDEF0444",
		TenantID:             "tenant-alpha",
		WorkflowDefinitionID: wf.ID,
		Source:               "TRAVEL_APP",
		ExternalID:           "booking_2002",
		CorrelationID:        "55555555-5555-4555-8555-555555555555",
		Payload:              map[string]interface{}{"trip": "NYC"},
	}

	state, err := svc.StartSequentialSaga(ctx, startInput)
	if err != nil {
		t.Fatalf("failed to start saga: %v", err)
	}

	_ = svc.HandleStepCompletion(ctx, "tenant-alpha", state.ID, 0, true, map[string]interface{}{"hotel": "ok"}, false)

	err = svc.HandleStepCompletion(ctx, "tenant-alpha", state.ID, 1, false, nil, false)
	if err != nil {
		t.Fatalf("failed handling failure: %v", err)
	}

	s, _ := svc.GetSaga(ctx, "tenant-alpha", state.ID)
	if s.Phase != contracts.PhaseCompensating {
		t.Fatalf("expected PhaseCompensating, got %s", s.Phase)
	}
	if s.CurrentStepIndex != 0 {
		t.Fatalf("expected rollback target step 0, got %d", s.CurrentStepIndex)
	}
	if len(dispatcher.Compensated) != 1 || dispatcher.Compensated[0] != 0 {
		t.Fatalf("expected step 0 to be compensated, got: %v", dispatcher.Compensated)
	}
}

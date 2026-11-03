package tests

import (
	"context"
	"strings"
	"testing"

	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	workflowAdapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowService "distributed-transaction-engine/src/features/workflow-definitions/service"
	"distributed-transaction-engine/src/shared/errors"
)

type MockStepDispatcher struct {
	failOnStepID        string
	failWithAmbiguous   bool
	failOnCompStepID    string
	executedSteps       []string
	compensatedSteps    []string
	recordedFwdKeys     []string
	recordedCompKeys    []string
	receivedCompOutputs []map[string]interface{}
}

func (m *MockStepDispatcher) DispatchStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}, idempotencyKey string) (map[string]interface{}, error) {
	m.executedSteps = append(m.executedSteps, stepDef.StepID)
	m.recordedFwdKeys = append(m.recordedFwdKeys, idempotencyKey)

	if stepDef.StepID == m.failOnStepID {
		if m.failWithAmbiguous {
			return nil, &sagaAdapters.AmbiguousStepError{
				StatusCode: 504,
				Message:    "downstream 504 gateway timeout",
			}
		}
		return nil, errors.NewInternalError("downstream step execution failed")
	}
	return map[string]interface{}{"status": "success", "step": stepDef.StepID, "charge_id": "ch_mock_123"}, nil
}

func (m *MockStepDispatcher) CompensateStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}, output map[string]interface{}, idempotencyKey string) error {
	m.compensatedSteps = append(m.compensatedSteps, stepDef.StepID)
	m.recordedCompKeys = append(m.recordedCompKeys, idempotencyKey)
	m.receivedCompOutputs = append(m.receivedCompOutputs, output)
	if stepDef.StepID == m.failOnCompStepID {
		return errors.NewInternalError("downstream compensation service unavailable")
	}
	return nil
}

func TestSagaService_SuccessfulDAGExecution(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_svc_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "booking-dag",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_booking_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000001",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "reserve_flight",
				StepName:             "Reserve Flight",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://flight-service/reserve",
				CompensationAction:   "CANCEL_FLIGHT",
				CompensationEndpoint: "http://flight-service/cancel",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "charge_card",
				StepName:             "Charge Card",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://payment-service/charge",
				TimeoutSeconds:       60,
				Dependencies:         []string{"reserve_flight"},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	instance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "booking-dag",
		WorkflowVersion: 1,
		Source:          "SERVICE_TEST",
		ExternalID:      "ext_saga_exec_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000001",
		InitialPayload:  map[string]interface{}{"amount": 250},
	})
	if err != nil {
		t.Fatalf("expected successful execution, got error: %v", err)
	}

	if instance.CurrentState != sagaContracts.SagaStateCompleted {
		t.Fatalf("expected COMPLETED state, got: %s", instance.CurrentState)
	}
	if len(instance.Steps) != 2 {
		t.Fatalf("expected 2 step records, got %d", len(instance.Steps))
	}
	for _, step := range instance.Steps {
		if step.ExecutionState != sagaContracts.StepStateSucceeded {
			t.Errorf("step %s has state %s, expected SUCCEEDED", step.StepID, step.ExecutionState)
		}
	}
}

func TestSagaService_PostPivotFailureNeverCompensatesPrePivotSteps(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_postpivot_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "checkout-fulfillment-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_postpivot_wf_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000099",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "reserve_inventory",
				StepName:             "Reserve Stock",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://inventory/reserve",
				CompensationAction:   "RELEASE_INVENTORY",
				CompensationEndpoint: "http://inventory/release",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "charge_card",
				StepName:             "Charge Credit Card (Pivot)",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://payments/charge",
				TimeoutSeconds:       30,
				Dependencies:         []string{"reserve_inventory"},
			},
			{
				StepID:               "create_shipment",
				StepName:             "Create Shipment Label",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://shipping/create",
				CompensationAction:   "CANCEL_SHIPMENT",
				CompensationEndpoint: "http://shipping/cancel",
				TimeoutSeconds:       30,
				Dependencies:         []string{"charge_card"},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{
		failOnStepID: "create_shipment",
	}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	instance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "checkout-fulfillment-flow",
		WorkflowVersion: 1,
		Source:          "POSTPIVOT_TEST",
		ExternalID:      "ext_postpivot_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000099",
		InitialPayload:  map[string]interface{}{"order_id": 555},
	})
	if err == nil {
		t.Fatalf("expected error from failed shipment step, got nil")
	}

	if instance.CurrentState != sagaContracts.SagaStateStuckForward {
		t.Fatalf("expected STUCK_FORWARD state, got: %s", instance.CurrentState)
	}

	if len(dispatcher.compensatedSteps) != 0 {
		t.Fatalf("CRITICAL BUG DETECTED: post-pivot failure compensated pre-pivot steps: %v", dispatcher.compensatedSteps)
	}
}

func TestSagaService_AmbiguousTimeoutMarkedUnknownAndCompensated(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_unknown_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "ambiguous-test-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_ambig_wf_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000088",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "reserve_seat",
				StepName:             "Reserve Seat",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://airline/reserve",
				CompensationAction:   "RELEASE_SEAT",
				CompensationEndpoint: "http://airline/release",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "charge_card",
				StepName:             "Charge Card",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://payments/charge",
				TimeoutSeconds:       30,
				Dependencies:         []string{"reserve_seat"},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{
		failOnStepID:      "reserve_seat",
		failWithAmbiguous: true,
	}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	instance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "ambiguous-test-flow",
		WorkflowVersion: 1,
		Source:          "UNKNOWN_TEST",
		ExternalID:      "ext_ambig_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000088",
		InitialPayload:  map[string]interface{}{"flight": "AI-101"},
	})
	if err == nil {
		t.Fatalf("expected error from ambiguous 504 timeout, got nil")
	}

	if instance.CurrentState != sagaContracts.SagaStateFailedCompensated {
		t.Fatalf("expected FAILED_COMPENSATED, got: %s", instance.CurrentState)
	}

	if len(dispatcher.compensatedSteps) != 1 || dispatcher.compensatedSteps[0] != "reserve_seat" {
		t.Fatalf("expected ambiguous step reserve_seat to be compensated, got: %v", dispatcher.compensatedSteps)
	}
}

func TestSagaService_BusinessKeyDeduplication(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_dedup_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "dedup-test-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_dedup_wf_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000077",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "validate_user",
				StepName:             "Validate User",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://users/validate",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	firstInstance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "dedup-test-flow",
		WorkflowVersion: 1,
		Source:          "DEDUP_TEST",
		ExternalID:      "ext_client_order_999",
		CorrelationID:   "c2000000-0000-0000-0000-000000000077",
		InitialPayload:  map[string]interface{}{"price": 100},
	})
	if err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	replayedInstance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "dedup-test-flow",
		WorkflowVersion: 1,
		Source:          "DEDUP_TEST",
		ExternalID:      "ext_client_order_999",
		CorrelationID:   "c3000000-0000-0000-0000-000000000077",
		InitialPayload:  map[string]interface{}{"price": 100},
	})
	if err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
	if replayedInstance.ID != firstInstance.ID {
		t.Fatalf("expected same saga ID on idempotent replay, got %s vs %s", replayedInstance.ID, firstInstance.ID)
	}

	_, conflictErr := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "dedup-test-flow",
		WorkflowVersion: 1,
		Source:          "DEDUP_TEST",
		ExternalID:      "ext_client_order_999",
		CorrelationID:   "c4000000-0000-0000-0000-000000000077",
		InitialPayload:  map[string]interface{}{"price": 999},
	})
	if conflictErr == nil {
		t.Fatalf("expected 409 conflict error when payload differs, got nil")
	}
	if appErr, ok := conflictErr.(*errors.ApplicationError); !ok || appErr.StatusCode != 409 {
		t.Fatalf("expected 409 conflict error, got: %v", conflictErr)
	}
}

func TestSagaService_DistinctIdempotencyKeysAndOutputPropagation(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_keys_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "keys-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_keys_wf_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000066",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "charge_step",
				StepName:             "Charge",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://payment/charge",
				CompensationAction:   "REFUND",
				CompensationEndpoint: "http://payment/refund",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "fail_step",
				StepName:             "Fail",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://fail/run",
				TimeoutSeconds:       30,
				Dependencies:         []string{"charge_step"},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{
		failOnStepID: "fail_step",
	}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	_, err = svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "keys-flow",
		WorkflowVersion: 1,
		Source:          "KEYS_TEST",
		ExternalID:      "ext_keys_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000066",
		InitialPayload:  map[string]interface{}{"amount": 50},
	})
	if err == nil {
		t.Fatalf("expected error from fail_step")
	}

	if len(dispatcher.recordedFwdKeys) == 0 || !strings.HasSuffix(dispatcher.recordedFwdKeys[0], ":fwd") {
		t.Fatalf("expected forward key to end in :fwd, got: %v", dispatcher.recordedFwdKeys)
	}
	if len(dispatcher.recordedCompKeys) == 0 || !strings.HasSuffix(dispatcher.recordedCompKeys[0], ":comp") {
		t.Fatalf("expected compensation key to end in :comp, got: %v", dispatcher.recordedCompKeys)
	}

	if len(dispatcher.receivedCompOutputs) == 0 {
		t.Fatalf("expected compensation to receive forward step output")
	}
	if dispatcher.receivedCompOutputs[0]["charge_id"] != "ch_mock_123" {
		t.Fatalf("expected charge_id in compensation output payload, got: %v", dispatcher.receivedCompOutputs[0])
	}
}

func TestSagaService_PartialCompensationStallsInCompensationStuck(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_partial_comp_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "partial-comp-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_partial_wf_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000088",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "step_flight",
				StepName:             "Flight",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://flight/hold",
				CompensationAction:   "RELEASE_FLIGHT",
				CompensationEndpoint: "http://flight/release",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "step_hotel",
				StepName:             "Hotel",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://hotel/hold",
				CompensationAction:   "RELEASE_HOTEL",
				CompensationEndpoint: "http://hotel/release",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "step_charge",
				StepName:             "Charge",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://payment/charge",
				TimeoutSeconds:       30,
				Dependencies:         []string{"step_flight", "step_hotel"},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{
		failOnStepID:     "step_charge",
		failOnCompStepID: "step_flight",
	}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	instance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "partial-comp-flow",
		WorkflowVersion: 1,
		Source:          "CHAOS_TEST",
		ExternalID:      "ext_partial_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000088",
		InitialPayload:  map[string]interface{}{"booking_id": "b_123"},
	})
	if err == nil {
		t.Fatalf("expected step_charge failure")
	}

	if instance.CurrentState != sagaContracts.SagaStateCompensationStuck {
		t.Fatalf("expected saga state COMPENSATION_STUCK, got %s", instance.CurrentState)
	}
}

func TestSagaService_AsymmetricDiamondDAGWithMixedErrorsAndDrain(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_diamond_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "diamond-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_diamond_wf_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000099",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "step_root",
				StepName:             "Root Step",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://service/root",
				CompensationAction:   "UNDO_ROOT",
				CompensationEndpoint: "http://service/undo-root",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "step_branch_fail",
				StepName:             "Branch Fail Step",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://service/branch-fail",
				CompensationAction:   "UNDO_BRANCH",
				CompensationEndpoint: "http://service/undo-branch",
				TimeoutSeconds:       30,
				Dependencies:         []string{"step_root"},
			},
			{
				StepID:               "step_terminal_join",
				StepName:             "Terminal Join",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://service/join",
				TimeoutSeconds:       30,
				Dependencies:         []string{"step_branch_fail"},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{
		failOnStepID:      "step_branch_fail",
		failWithAmbiguous: true,
	}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	instance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "diamond-flow",
		WorkflowVersion: 1,
		Source:          "CHAOS_TEST",
		ExternalID:      "ext_diamond_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000099",
		InitialPayload:  map[string]interface{}{"order_id": "ord_999"},
	})
	if err == nil {
		t.Fatalf("expected step_branch_fail ambiguous error")
	}

	if instance.CurrentState != sagaContracts.SagaStateFailedCompensated {
		t.Fatalf("expected saga state FAILED_COMPENSATED, got %s", instance.CurrentState)
	}

	var joinStepExec *sagaContracts.SagaStepExecution
	for i := range instance.Steps {
		if instance.Steps[i].StepID == "step_terminal_join" {
			joinStepExec = &instance.Steps[i]
			break
		}
	}
	if joinStepExec == nil {
		t.Fatalf("expected step_terminal_join in instance steps")
	}
	if joinStepExec.ExecutionState != sagaContracts.StepStateNotStarted {
		t.Fatalf("expected unreached join step to be NOT_STARTED, got %s", joinStepExec.ExecutionState)
	}
}

func TestSagaService_ConflictingPayloadExternalIDRejection(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_conflict_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "conflict-wf",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_conflict_wf_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000077",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "simple_step",
				StepName:             "Simple",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://service/simple",
				CompensationAction:   "UNDO_SIMPLE",
				CompensationEndpoint: "http://service/undo-simple",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	_, err = svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "conflict-wf",
		WorkflowVersion: 1,
		Source:          "CONFLICT_TEST",
		ExternalID:      "ext_unique_order_555",
		CorrelationID:   "c2000000-0000-0000-0000-000000000077",
		InitialPayload:  map[string]interface{}{"price": 100},
	})
	if err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	_, conflictErr := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "conflict-wf",
		WorkflowVersion: 1,
		Source:          "CONFLICT_TEST",
		ExternalID:      "ext_unique_order_555",
		CorrelationID:   "c2000000-0000-0000-0000-000000000078",
		InitialPayload:  map[string]interface{}{"price": 99999},
	})
	if conflictErr == nil {
		t.Fatalf("expected conflict error on altered payload with identical external_id")
	}

	appErr, ok := conflictErr.(*errors.ApplicationError)
	if !ok || appErr.StatusCode != 409 {
		t.Fatalf("expected HTTP 409 Conflict, got: %v", conflictErr)
	}
}

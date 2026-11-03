package tests

import (
	"context"
	"testing"

	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	workflowAdapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowService "distributed-transaction-engine/src/features/workflow-definitions/service"
	"distributed-transaction-engine/src/shared/errors"
)

type MockStepDispatcher struct {
	failOnStepID      string
	executedSteps     []string
	compensatedSteps  []string
}

func (m *MockStepDispatcher) DispatchStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}) (map[string]interface{}, error) {
	m.executedSteps = append(m.executedSteps, stepDef.StepID)
	if stepDef.StepID == m.failOnStepID {
		return nil, errors.NewInternalError("downstream step execution failed")
	}
	return map[string]interface{}{"status": "success", "step": stepDef.StepID}, nil
}

func (m *MockStepDispatcher) CompensateStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}) error {
	m.compensatedSteps = append(m.compensatedSteps, stepDef.StepID)
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

func TestSagaService_FailureAndReverseCompensation(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_comp_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "order-fulfillment",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_fulfill_001",
		CorrelationID: "c3000000-0000-0000-0000-000000000001",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "reserve_inventory",
				StepName:             "Reserve Inventory",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://inventory/reserve",
				CompensationAction:   "RELEASE_INVENTORY",
				CompensationEndpoint: "http://inventory/release",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
			{
				StepID:               "charge_customer",
				StepName:             "Charge Customer",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://billing/charge",
				TimeoutSeconds:       30,
				Dependencies:         []string{"reserve_inventory"},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{
		failOnStepID: "charge_customer",
	}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	instance, err := svc.ExecuteSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "order-fulfillment",
		WorkflowVersion: 1,
		Source:          "COMP_TEST",
		ExternalID:      "ext_comp_exec_001",
		CorrelationID:   "c4000000-0000-0000-0000-000000000001",
		InitialPayload:  map[string]interface{}{"order_id": 99},
	})
	if err == nil {
		t.Fatalf("expected error from failed step, got nil")
	}

	if instance.CurrentState != sagaContracts.SagaStateFailed {
		t.Fatalf("expected FAILED state, got: %s", instance.CurrentState)
	}

	if len(dispatcher.compensatedSteps) != 1 || dispatcher.compensatedSteps[0] != "reserve_inventory" {
		t.Fatalf("expected reserve_inventory to be compensated, got: %v", dispatcher.compensatedSteps)
	}

	for _, step := range instance.Steps {
		if step.StepID == "reserve_inventory" && step.ExecutionState != sagaContracts.StepStateCompensated {
			t.Errorf("expected reserve_inventory to be COMPENSATED, got: %s", step.ExecutionState)
		}
		if step.StepID == "charge_customer" && step.ExecutionState != sagaContracts.StepStateFailed {
			t.Errorf("expected charge_customer to be FAILED, got: %s", step.ExecutionState)
		}
	}
}

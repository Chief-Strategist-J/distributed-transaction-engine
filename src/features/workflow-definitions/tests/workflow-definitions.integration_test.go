package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
)

func TestIntegration_WorkflowDefinitionsRepositoryLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)

	tenantID := "tenant_integration_01"
	name := "order-fulfillment-pipeline"
	version := int64(1)

	steps := []contracts.StepDefinition{
		{
			StepID:               "validate_order",
			StepName:             "Validate Order",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://order-service/validate",
			CompensationAction:   "INVALIDATE",
			CompensationEndpoint: "http://order-service/invalidate",
			TimeoutSeconds:       10,
			MaxRetries:           2,
			Dependencies:         []string{},
		},
		{
			StepID:               "hold_inventory",
			StepName:             "Hold Inventory",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://inventory-service/hold",
			CompensationAction:   "RELEASE_HOLD",
			CompensationEndpoint: "http://inventory-service/release",
			TimeoutSeconds:       15,
			MaxRetries:           3,
			Dependencies:         []string{"validate_order"},
		},
		{
			StepID:               "capture_payment",
			StepName:             "Capture Payment",
			IsPivot:              true,
			ActionType:           "REST",
			ActionEndpoint:       "http://payment-gateway/capture",
			CompensationAction:   "",
			CompensationEndpoint: "",
			TimeoutSeconds:       30,
			MaxRetries:           1,
			Dependencies:         []string{"hold_inventory"},
		},
	}

	input := contracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          name,
		Version:       version,
		Source:        "TEST_SUITE",
		ExternalID:    "ext_wf_integration_001",
		CorrelationID: "c1000000-0000-0000-0000-000000000001",
		Steps:         steps,
	}

	created, err := svc.RegisterWorkflow(ctx, input)
	if err != nil {
		t.Fatalf("failed to register valid workflow: %v", err)
	}

	if created.TotalSteps != 3 {
		t.Fatalf("expected 3 total steps, got %d", created.TotalSteps)
	}
	if created.Status != "ACTIVE" {
		t.Fatalf("expected status ACTIVE, got %s", created.Status)
	}

	fetchedByID, err := svc.GetWorkflow(ctx, tenantID, created.ID)
	if err != nil {
		t.Fatalf("failed to fetch workflow by id: %v", err)
	}
	if fetchedByID.SpecHash != created.SpecHash {
		t.Fatalf("spec hash mismatch: %s vs %s", fetchedByID.SpecHash, created.SpecHash)
	}

	fetchedByNameVer, err := svc.GetWorkflowByNameAndVersion(ctx, tenantID, name, version)
	if err != nil {
		t.Fatalf("failed to fetch workflow by name and version: %v", err)
	}
	if fetchedByNameVer.ID != created.ID {
		t.Fatalf("id mismatch: %s vs %s", fetchedByNameVer.ID, created.ID)
	}

	list, err := svc.ListWorkflows(ctx, tenantID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list workflows: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 listed workflow, got %d", len(list))
	}
}

func TestIntegration_ImmutabilityAndCollisionRejection(t *testing.T) {
	ctx := context.Background()
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)

	tenantID := "tenant_collision"
	name := "payment-flow"

	baseSteps := []contracts.StepDefinition{
		{
			StepID:               "authorize",
			StepName:             "Authorize Funds",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://payment/auth",
			CompensationAction:   "VOID",
			CompensationEndpoint: "http://payment/void",
			TimeoutSeconds:       10,
			MaxRetries:           2,
			Dependencies:         []string{},
		},
		{
			StepID:               "settle",
			StepName:             "Settle Funds",
			IsPivot:              true,
			ActionType:           "REST",
			ActionEndpoint:       "http://payment/settle",
			TimeoutSeconds:       20,
			MaxRetries:           0,
			Dependencies:         []string{"authorize"},
		},
	}

	_, err := svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          name,
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_1",
		CorrelationID: "c0000000-0000-0000-0000-000000000001",
		Steps:         baseSteps,
	})
	if err != nil {
		t.Fatalf("initial registration failed: %v", err)
	}

	_, err = svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          name,
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_2",
		CorrelationID: "c0000000-0000-0000-0000-000000000002",
		Steps:         baseSteps,
	})
	if err == nil {
		t.Fatalf("expected duplicate version registration rejection, got nil")
	}

	_, err = svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          name,
		Version:       2,
		Source:        "TEST",
		ExternalID:    "ext_3",
		CorrelationID: "c0000000-0000-0000-0000-000000000003",
		Steps:         baseSteps,
	})
	if err == nil {
		t.Fatalf("expected duplicate hash rejection under new version, got nil")
	}
}

func TestIntegration_ConcurrentRegistrations(t *testing.T) {
	ctx := context.Background()
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)

	concurrency := 20
	var wg sync.WaitGroup
	errCount := 0
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
				TenantID:      "tenant_concurrent",
				Name:          fmt.Sprintf("wf_concurrent_%02d", idx),
				Version:       1,
				Source:        "CONCURRENT_TEST",
				ExternalID:    fmt.Sprintf("ext_conc_%d", idx),
				CorrelationID: fmt.Sprintf("c0000000-0000-0000-0000-%012d", idx),
				Steps: []contracts.StepDefinition{
					{
						StepID:         fmt.Sprintf("step_%d", idx),
						StepName:       "Step Single",
						IsPivot:        true,
						ActionType:     "REST",
						ActionEndpoint: "http://service/run",
						TimeoutSeconds: 10,
						Dependencies:   []string{},
					},
				},
			})
			mu.Lock()
			if err != nil {
				errCount++
			} else {
				successCount++
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if successCount != concurrency {
		t.Fatalf("expected %d successes, got %d (errors: %d)", concurrency, successCount, errCount)
	}
}


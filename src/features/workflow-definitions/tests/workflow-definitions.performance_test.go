package tests

import (
	"context"
	"fmt"
	"testing"

	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	rules "distributed-transaction-engine/src/features/workflow-definitions/domain/rules"
)

func BenchmarkDAGTopologyValidation_SmallDAG(b *testing.B) {
	steps := []contracts.StepDefinition{
		{
			StepID:         "step_1",
			StepName:       "Step 1",
			IsPivot:        false,
			ActionType:     "REST",
			ActionEndpoint: "http://svc/1",
			Dependencies:   []string{},
		},
		{
			StepID:         "step_2",
			StepName:       "Step 2",
			IsPivot:        false,
			ActionType:     "REST",
			ActionEndpoint: "http://svc/2",
			Dependencies:   []string{"step_1"},
		},
		{
			StepID:         "step_3",
			StepName:       "Step 3",
			IsPivot:        true,
			ActionType:     "REST",
			ActionEndpoint: "http://svc/3",
			Dependencies:   []string{"step_2"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := rules.ValidateDAGTopology(steps)
		if !res.IsValid {
			b.Fatalf("expected valid DAG")
		}
	}
}

func BenchmarkDAGTopologyValidation_LargeDAG(b *testing.B) {
	stepCount := 200
	steps := make([]contracts.StepDefinition, stepCount)

	for i := 0; i < stepCount; i++ {
		stepID := fmt.Sprintf("step_%04d", i)
		deps := []string{}
		if i > 0 {
			deps = append(deps, fmt.Sprintf("step_%04d", i-1))
		}
		steps[i] = contracts.StepDefinition{
			StepID:         stepID,
			StepName:       stepID,
			IsPivot:        (i == stepCount-1),
			ActionType:     "REST",
			ActionEndpoint: "http://svc",
			Dependencies:   deps,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := rules.ValidateDAGTopology(steps)
		if !res.IsValid {
			b.Fatalf("expected valid large DAG")
		}
	}
}

func BenchmarkWorkflowRegistrationThroughput(b *testing.B) {
	ctx := context.Background()
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)

	steps := []contracts.StepDefinition{
		{
			StepID:               "auth",
			StepName:             "Auth",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://auth",
			CompensationAction:   "REVOKE",
			CompensationEndpoint: "http://auth/revoke",
			TimeoutSeconds:       10,
			Dependencies:         []string{},
		},
		{
			StepID:         "capture",
			StepName:       "Capture",
			IsPivot:        true,
			ActionType:     "REST",
			ActionEndpoint: "http://capture",
			TimeoutSeconds: 20,
			Dependencies:   []string{"auth"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("bench_wf_%d", i)
		_, err := svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
			TenantID:      "tenant_bench",
			Name:          name,
			Version:       1,
			Source:        "BENCHMARK",
			ExternalID:    fmt.Sprintf("ext_bench_%d", i),
			CorrelationID: "c0000000-0000-0000-0000-000000000001",
			Steps:         steps,
		})
		if err != nil {
			b.Fatalf("benchmark registration failed: %v", err)
		}
	}
}


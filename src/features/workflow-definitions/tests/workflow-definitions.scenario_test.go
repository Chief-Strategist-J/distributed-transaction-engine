package tests

import (
	"context"
	"fmt"
	"testing"

	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
)

func TestScenario_MultiBranchDiamondDAGResolution(t *testing.T) {
	ctx := context.Background()
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)

	diamondSteps := []contracts.StepDefinition{
		{
			StepID:               "start_node",
			StepName:             "Start Ingestion Node",
			IsPivot:              false,
			ActionType:           "INTERNAL",
			ActionEndpoint:       "http://core/start",
			CompensationAction:   "CLEANUP_START",
			CompensationEndpoint: "http://core/cleanup",
			TimeoutSeconds:       10,
			Dependencies:         []string{},
		},
		{
			StepID:               "branch_left",
			StepName:             "Branch Left KYC Verify",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://kyc/verify",
			CompensationAction:   "CANCEL_KYC",
			CompensationEndpoint: "http://kyc/cancel",
			TimeoutSeconds:       20,
			Dependencies:         []string{"start_node"},
		},
		{
			StepID:               "branch_right",
			StepName:             "Branch Right AML Screen",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://aml/screen",
			CompensationAction:   "DISMISS_AML",
			CompensationEndpoint: "http://aml/dismiss",
			TimeoutSeconds:       20,
			Dependencies:         []string{"start_node"},
		},
		{
			StepID:               "join_pivot",
			StepName:             "Join Account Activation",
			IsPivot:              true,
			ActionType:           "GRPC",
			ActionEndpoint:       "account.service:50051",
			TimeoutSeconds:       45,
			Dependencies:         []string{"branch_left", "branch_right"},
		},
	}

	def, err := svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
		TenantID:      "tenant_diamond",
		Name:          "compliance-onboarding-dag",
		Version:       1,
		Source:        "SCENARIO_RUNNER",
		ExternalID:    "ext_diamond_001",
		CorrelationID: "c3000000-0000-0000-0000-000000000001",
		Steps:         diamondSteps,
	})
	if err != nil {
		t.Fatalf("failed to register diamond DAG: %v", err)
	}

	if def.TotalSteps != 4 {
		t.Fatalf("expected 4 steps in diamond DAG, got %d", def.TotalSteps)
	}
}

func TestScenario_DeepChainDAGScalability(t *testing.T) {
	ctx := context.Background()
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)

	chainDepth := 50
	steps := make([]contracts.StepDefinition, chainDepth)

	for i := 0; i < chainDepth; i++ {
		stepID := fmt.Sprintf("chain_step_%03d", i)
		deps := []string{}
		if i > 0 {
			deps = append(deps, fmt.Sprintf("chain_step_%03d", i-1))
		}

		isPivot := (i == chainDepth-1)
		compAction := fmt.Sprintf("ROLLBACK_%03d", i)
		compEndpoint := fmt.Sprintf("http://service/undo/%03d", i)
		if isPivot {
			compAction = ""
			compEndpoint = ""
		}

		steps[i] = contracts.StepDefinition{
			StepID:               stepID,
			StepName:             fmt.Sprintf("Chain Step %d", i),
			IsPivot:              isPivot,
			ActionType:           "RPC",
			ActionEndpoint:       fmt.Sprintf("http://chain-node/%03d", i),
			CompensationAction:   compAction,
			CompensationEndpoint: compEndpoint,
			TimeoutSeconds:       10,
			Dependencies:         deps,
		}
	}

	def, err := svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
		TenantID:      "tenant_deep_chain",
		Name:          "deep-sequential-chain-dag",
		Version:       1,
		Source:        "SCENARIO_RUNNER",
		ExternalID:    "ext_deep_chain_001",
		CorrelationID: "c4000000-0000-0000-0000-000000000001",
		Steps:         steps,
	})
	if err != nil {
		t.Fatalf("failed to register deep chain DAG: %v", err)
	}

	if def.TotalSteps != chainDepth {
		t.Fatalf("expected %d steps, got %d", chainDepth, def.TotalSteps)
	}
}

func TestScenario_SelfLoopCycleRejected(t *testing.T) {
	ctx := context.Background()
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)

	selfLoopSteps := []contracts.StepDefinition{
		{
			StepID:               "self_loop_step",
			StepName:             "Self Looping Step",
			IsPivot:              true,
			ActionType:           "REST",
			ActionEndpoint:       "http://self/loop",
			TimeoutSeconds:       10,
			Dependencies:         []string{"self_loop_step"},
		},
	}

	_, err := svc.RegisterWorkflow(ctx, contracts.CreateWorkflowDefinitionInput{
		TenantID:      "tenant_self_loop",
		Name:          "self-loop-flow",
		Version:       1,
		Source:        "SCENARIO_RUNNER",
		ExternalID:    "ext_self_loop",
		CorrelationID: "c5000000-0000-0000-0000-000000000001",
		Steps:         selfLoopSteps,
	})
	if err == nil {
		t.Fatalf("expected self-loop rejection, got nil")
	}
}


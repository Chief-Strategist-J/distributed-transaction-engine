package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	rules "distributed-transaction-engine/src/features/compensation-engine/domain/rules"
)

func TestBuildCompensationPlan_Reversal(t *testing.T) {
	forwardSteps := []contracts.ForwardStepExecutionRecord{
		{
			StepID:             "step_1_reserve",
			StepName:           "Reserve Inventory",
			IsPivot:            false,
			ForwardOutcome:     "SUCCEEDED",
			CompensationAction: "HTTP_RELEASE_STOCK",
			ExecutionOrder:     1,
		},
		{
			StepID:             "step_2_auth",
			StepName:           "Authorize Card",
			IsPivot:            false,
			ForwardOutcome:     "SUCCEEDED",
			CompensationAction: "HTTP_VOID_AUTH",
			ExecutionOrder:     2,
		},
		{
			StepID:             "step_3_tax",
			StepName:           "Calculate Tax",
			IsPivot:            false,
			ForwardOutcome:     "SKIPPED",
			CompensationAction: "",
			ExecutionOrder:     3,
		},
	}

	plan, err := rules.BuildCompensationPlan(forwardSteps)
	if err != nil {
		t.Fatalf("unexpected error building compensation plan: %v", err)
	}

	if len(plan) != 2 {
		t.Fatalf("expected 2 compensable steps, got %d", len(plan))
	}

	if plan[0].StepID != "step_2_auth" || plan[0].ExecutionOrder != 1 {
		t.Fatalf("expected step_2_auth to be first in reverse plan, got: %+v", plan[0])
	}
	if plan[1].StepID != "step_1_reserve" || plan[1].ExecutionOrder != 2 {
		t.Fatalf("expected step_1_reserve to be second in reverse plan, got: %+v", plan[1])
	}
}

func TestBuildCompensationPlan_PivotRejection(t *testing.T) {
	forwardSteps := []contracts.ForwardStepExecutionRecord{
		{
			StepID:             "step_1_reserve",
			IsPivot:            false,
			ForwardOutcome:     "SUCCEEDED",
			CompensationAction: "HTTP_RELEASE_STOCK",
		},
		{
			StepID:             "step_2_settle",
			IsPivot:            true,
			ForwardOutcome:     "SUCCEEDED",
			CompensationAction: "CANNOT_UNDO",
		},
	}

	_, err := rules.BuildCompensationPlan(forwardSteps)
	if err == nil {
		t.Fatal("expected error: CE-02 pivot succeeded cannot enter compensation plan")
	}
}

func TestEvaluatePlanCompletion(t *testing.T) {
	steps := []contracts.SagaStepCompensationInstance{
		{StepID: "s1", Status: contracts.StepStatusCompensated},
		{StepID: "s2", Status: contracts.StepStatusPending},
	}
	state := rules.EvaluatePlanCompletion(steps)
	if state != contracts.PlanStateCompensating {
		t.Fatalf("expected PlanStateCompensating while pending, got %s", state)
	}

	steps[1].Status = contracts.StepStatusCompensated
	state = rules.EvaluatePlanCompletion(steps)
	if state != contracts.PlanStateFullyCompensated {
		t.Fatalf("expected PlanStateFullyCompensated when all compensated, got %s", state)
	}

	steps[1].Status = contracts.StepStatusNotCompensable
	state = rules.EvaluatePlanCompletion(steps)
	if state != contracts.PlanStateCompensatedWithResiduals {
		t.Fatalf("expected PlanStateCompensatedWithResiduals when not compensable residual exists, got %s", state)
	}

	steps[1].Status = contracts.StepStatusNeedsAttention
	state = rules.EvaluatePlanCompletion(steps)
	if state != contracts.PlanStateCompensatingNeedsAttention {
		t.Fatalf("expected PlanStateCompensatingNeedsAttention on failure, got %s", state)
	}
}

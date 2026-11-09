package rules

import (
	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

func BuildCompensationPlan(
	forwardSteps []contracts.ForwardStepExecutionRecord,
) ([]contracts.ForwardStepExecutionRecord, error) {
	for _, step := range forwardSteps {
		if step.IsPivot && (step.ForwardOutcome == "SUCCEEDED" || step.ForwardOutcome == "UNKNOWN") {
			return nil, errors.NewConflictError("CE-02: pivot step succeeded or open; compensation is forbidden")
		}
	}

	var compensableSteps []contracts.ForwardStepExecutionRecord
	for _, step := range forwardSteps {
		if step.ForwardOutcome == "NOT_EXECUTED" || step.ForwardOutcome == "SKIPPED" {
			continue
		}
		if step.CompensationAction == "" {
			continue
		}
		compensableSteps = append(compensableSteps, step)
	}

	n := len(compensableSteps)
	reversed := make([]contracts.ForwardStepExecutionRecord, n)
	for i := 0; i < n; i++ {
		reversed[i] = compensableSteps[n-1-i]
		reversed[i].ExecutionOrder = i + 1
	}

	return reversed, nil
}

func EvaluatePlanCompletion(
	steps []contracts.SagaStepCompensationInstance,
) contracts.CompensationPlanState {
	if len(steps) == 0 {
		return contracts.PlanStateFullyCompensated
	}

	hasPending := false
	hasNeedsAttention := false
	hasResiduals := false

	for _, step := range steps {
		switch step.Status {
		case contracts.StepStatusPending, contracts.StepStatusDispatched, contracts.StepStatusFailedRetryable:
			hasPending = true
		case contracts.StepStatusNeedsAttention:
			hasNeedsAttention = true
		case contracts.StepStatusNotCompensable, contracts.StepStatusPartiallyCompensated:
			hasResiduals = true
		case contracts.StepStatusCompensated:
		}
	}

	if hasPending {
		if hasNeedsAttention {
			return contracts.PlanStateCompensatingNeedsAttention
		}
		return contracts.PlanStateCompensating
	}

	if hasNeedsAttention {
		return contracts.PlanStateCompensatingNeedsAttention
	}

	if hasResiduals {
		return contracts.PlanStateCompensatedWithResiduals
	}

	return contracts.PlanStateFullyCompensated
}

package machines

import (
	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	rules "distributed-transaction-engine/src/features/compensation-engine/domain/rules"
)

type CompensationStateMachine struct{}

func NewCompensationStateMachine() *CompensationStateMachine {
	return &CompensationStateMachine{}
}

func (m *CompensationStateMachine) TransitionStep(
	currentStatus contracts.StepCompensationStatus,
	outcome contracts.StepCompensationStatus,
) contracts.StepCompensationStatus {
	switch currentStatus {
	case contracts.StepStatusPending, contracts.StepStatusDispatched, contracts.StepStatusFailedRetryable:
		return outcome
	default:
		return currentStatus
	}
}

func (m *CompensationStateMachine) EvaluatePlan(
	steps []contracts.SagaStepCompensationInstance,
) contracts.CompensationPlanState {
	return rules.EvaluatePlanCompletion(steps)
}

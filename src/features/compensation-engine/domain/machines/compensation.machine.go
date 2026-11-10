package machines

import (
	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	rules "distributed-transaction-engine/src/features/compensation-engine/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
)

type CompensationStateMachine struct{}

func NewCompensationStateMachine() *CompensationStateMachine {
	return &CompensationStateMachine{}
}

func (m *CompensationStateMachine) TransitionStep(
	currentStatus contracts.StepCompensationStatus,
	outcome contracts.StepCompensationStatus,
) contracts.StepCompensationStatus {
	defer observability.TraceEnter(nil)()
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
	defer observability.TraceEnter(nil)()
	return rules.EvaluatePlanCompletion(steps)
}

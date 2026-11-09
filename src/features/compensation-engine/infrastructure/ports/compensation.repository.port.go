package ports

import (
	"context"

	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
)

type CompensationRepositoryPort interface {
	CreateCompensationPlanAtomic(
		ctx context.Context,
		plan *contracts.SagaCompensationPlanInstance,
		steps []contracts.SagaStepCompensationInstance,
	) error

	GetCompensationPlan(
		ctx context.Context,
		tenantID string,
		sagaID string,
	) (*contracts.SagaCompensationPlanInstance, error)

	UpdateStepOutcome(
		ctx context.Context,
		tenantID string,
		planID string,
		stepID string,
		outcome contracts.StepCompensationStatus,
		residual map[string]interface{},
		errorMessage string,
	) error

	UpdatePlanCAS(
		ctx context.Context,
		tenantID string,
		planID string,
		expectedVersion int64,
		newState contracts.CompensationPlanState,
		compensatedSteps int,
		failedSteps int,
		residuals []map[string]interface{},
	) error
}

type CompensationDispatcherPort interface {
	DispatchCompensation(
		ctx context.Context,
		tenantID string,
		sagaID string,
		step contracts.SagaStepCompensationInstance,
	) error
}

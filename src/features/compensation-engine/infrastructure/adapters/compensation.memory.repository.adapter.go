package adapters

import (
	"context"
	"sync"
	"time"

	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type MemoryCompensationRepositoryAdapter struct {
	mu    sync.Mutex
	plans map[string]*contracts.SagaCompensationPlanInstance
	steps map[string][]contracts.SagaStepCompensationInstance
}

func NewMemoryCompensationRepositoryAdapter() *MemoryCompensationRepositoryAdapter {
	return &MemoryCompensationRepositoryAdapter{
		plans: make(map[string]*contracts.SagaCompensationPlanInstance),
		steps: make(map[string][]contracts.SagaStepCompensationInstance),
	}
}

func (a *MemoryCompensationRepositoryAdapter) CreateCompensationPlanAtomic(
	_ context.Context,
	plan *contracts.SagaCompensationPlanInstance,
	steps []contracts.SagaStepCompensationInstance,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, p := range a.plans {
		if p.TenantID == plan.TenantID && p.SagaID == plan.SagaID {
			return errors.NewConflictError("duplicate compensation plan for saga")
		}
	}

	pCopy := *plan
	pCopy.CreatedAt = time.Now().UTC()
	pCopy.UpdatedAt = pCopy.CreatedAt
	a.plans[plan.ID] = &pCopy
	a.steps[plan.ID] = append([]contracts.SagaStepCompensationInstance{}, steps...)
	return nil
}

func (a *MemoryCompensationRepositoryAdapter) GetCompensationPlan(
	_ context.Context,
	tenantID string,
	sagaID string,
) (*contracts.SagaCompensationPlanInstance, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, p := range a.plans {
		if p.TenantID == tenantID && (p.SagaID == sagaID || p.ID == sagaID) {
			pCopy := *p
			pCopy.Steps = append([]contracts.SagaStepCompensationInstance{}, a.steps[p.ID]...)
			return &pCopy, nil
		}
	}
	return nil, errors.NewNotFoundError("compensation plan not found")
}

func (a *MemoryCompensationRepositoryAdapter) UpdateStepOutcome(
	_ context.Context,
	tenantID string,
	planID string,
	stepID string,
	outcome contracts.StepCompensationStatus,
	residual map[string]interface{},
	errorMessage string,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	stepList, ok := a.steps[planID]
	if !ok {
		return errors.NewNotFoundError("plan not found")
	}

	for i := range stepList {
		if stepList[i].TenantID == tenantID && stepList[i].StepID == stepID {
			stepList[i].Status = outcome
			stepList[i].ResidualPayload = residual
			stepList[i].LastError = errorMessage
			stepList[i].UpdatedAt = time.Now().UTC()
			if outcome == contracts.StepStatusCompensated {
				now := time.Now().UTC()
				stepList[i].CompensatedAt = &now
			}
			return nil
		}
	}
	return errors.NewNotFoundError("step compensation not found")
}

func (a *MemoryCompensationRepositoryAdapter) UpdatePlanCAS(
	_ context.Context,
	tenantID string,
	planID string,
	expectedVersion int64,
	newState contracts.CompensationPlanState,
	compensatedSteps int,
	failedSteps int,
	residuals []map[string]interface{},
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	p, ok := a.plans[planID]
	if !ok || p.TenantID != tenantID {
		return errors.NewNotFoundError("plan not found")
	}
	if p.Version != expectedVersion {
		return errors.NewConflictError("stale version on plan CAS update")
	}

	p.CurrentState = newState
	p.CompensatedSteps = compensatedSteps
	p.FailedSteps = failedSteps
	p.Residuals = residuals
	p.Version++
	p.UpdatedAt = time.Now().UTC()
	if newState == contracts.PlanStateFullyCompensated || newState == contracts.PlanStateCompensatedWithResiduals {
		now := time.Now().UTC()
		p.CompletedAt = &now
	}
	return nil
}

type MemoryCompensationDispatcherAdapter struct {
	Dispatched []contracts.SagaStepCompensationInstance
}

func NewMemoryCompensationDispatcherAdapter() *MemoryCompensationDispatcherAdapter {
	return &MemoryCompensationDispatcherAdapter{
		Dispatched: make([]contracts.SagaStepCompensationInstance, 0),
	}
}

func (a *MemoryCompensationDispatcherAdapter) DispatchCompensation(
	_ context.Context,
	_ string,
	_ string,
	step contracts.SagaStepCompensationInstance,
) error {
	a.Dispatched = append(a.Dispatched, step)
	return nil
}

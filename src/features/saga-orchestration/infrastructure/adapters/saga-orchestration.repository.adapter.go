package adapters

import (
	"context"
	"sync"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type MemorySagaRepositoryAdapter struct {
	mu           sync.RWMutex
	storeByID    map[string]*sagaContracts.SagaInstance
	storeByPubID map[string]*sagaContracts.SagaInstance
	storeByExtID map[string]*sagaContracts.SagaInstance
}

func NewMemorySagaRepositoryAdapter() *MemorySagaRepositoryAdapter {
	return &MemorySagaRepositoryAdapter{
		storeByID:    make(map[string]*sagaContracts.SagaInstance),
		storeByPubID: make(map[string]*sagaContracts.SagaInstance),
		storeByExtID: make(map[string]*sagaContracts.SagaInstance),
	}
}

func (a *MemorySagaRepositoryAdapter) CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	idKey := instance.TenantID + ":" + instance.ID
	if _, exists := a.storeByID[idKey]; exists {
		return errors.NewConflictError("saga instance already exists with id: " + instance.ID)
	}

	extKey := instance.TenantID + ":" + instance.WorkflowDefinitionID + ":" + instance.ExternalID
	if _, exists := a.storeByExtID[extKey]; exists {
		return errors.NewConflictError("saga instance already exists with external id: " + instance.ExternalID)
	}

	copied := *instance
	copiedSteps := make([]sagaContracts.SagaStepExecution, len(instance.Steps))
	copy(copiedSteps, instance.Steps)
	copied.Steps = copiedSteps

	a.storeByID[idKey] = &copied
	if instance.PublicID != "" {
		pubKey := instance.TenantID + ":" + instance.PublicID
		a.storeByPubID[pubKey] = &copied
	}
	a.storeByExtID[extKey] = &copied
	return nil
}

func (a *MemorySagaRepositoryAdapter) GetSagaByID(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := tenantID + ":" + id
	instance, exists := a.storeByID[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga instance not found")
	}
	return instance, nil
}

func (a *MemorySagaRepositoryAdapter) GetSagaByPublicID(ctx context.Context, tenantID string, publicID string) (*sagaContracts.SagaInstance, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := tenantID + ":" + publicID
	instance, exists := a.storeByPubID[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga instance not found")
	}
	return instance, nil
}

func (a *MemorySagaRepositoryAdapter) GetSagaByExternalID(ctx context.Context, tenantID string, workflowDefinitionID string, externalID string) (*sagaContracts.SagaInstance, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := tenantID + ":" + workflowDefinitionID + ":" + externalID
	instance, exists := a.storeByExtID[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga instance not found")
	}
	return instance, nil
}

func (a *MemorySagaRepositoryAdapter) UpdateSagaState(ctx context.Context, tenantID string, id string, expectedVersion int64, nextState sagaContracts.SagaState) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := tenantID + ":" + id
	instance, exists := a.storeByID[key]
	if !exists {
		return errors.NewNotFoundError("saga instance not found")
	}

	if instance.Version != expectedVersion {
		return errors.NewConflictError("concurrency conflict: optimistic lock failed on saga state transition")
	}

	instance.CurrentState = nextState
	instance.Version++
	instance.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return nil
}

func (a *MemorySagaRepositoryAdapter) UpdateSagaStateWithFencing(ctx context.Context, tenantID string, id string, expectedVersion int64, fencingToken int64, nextState sagaContracts.SagaState) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := tenantID + ":" + id
	instance, exists := a.storeByID[key]
	if !exists {
		return errors.NewNotFoundError("saga instance not found")
	}

	if instance.Version != expectedVersion {
		return errors.NewConflictError("concurrency conflict: optimistic lock failed on saga state transition")
	}
	if instance.FencingToken != fencingToken {
		return errors.NewConflictError("fencing conflict: stale worker fencing token")
	}

	instance.CurrentState = nextState
	instance.Version++
	instance.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return nil
}

func (a *MemorySagaRepositoryAdapter) AcquireNextLease(ctx context.Context, workerID string, leaseDuration time.Duration) (*sagaContracts.SagaInstance, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now().UTC()
	for _, inst := range a.storeByID {
		isActionable := inst.CurrentState == sagaContracts.SagaStateCreated ||
			inst.CurrentState == sagaContracts.SagaStateRunning ||
			inst.CurrentState == sagaContracts.SagaStateRollingForward ||
			inst.CurrentState == sagaContracts.SagaStateCompensating

		if !isActionable {
			continue
		}

		isAvailable := false
		if inst.LeaseOwner == "" || inst.LeaseUntil == "" || inst.LeaseOwner == workerID {
			isAvailable = true
		} else {
			until, err := time.Parse(time.RFC3339Nano, inst.LeaseUntil)
			if err == nil && now.After(until) {
				isAvailable = true
			}
		}

		if isAvailable {
			inst.LeaseOwner = workerID
			inst.LeaseUntil = now.Add(leaseDuration).Format(time.RFC3339Nano)
			inst.FencingToken++
			inst.UpdatedAt = now.Format(time.RFC3339Nano)

			copied := *inst
			copiedSteps := make([]sagaContracts.SagaStepExecution, len(inst.Steps))
			copy(copiedSteps, inst.Steps)
			copied.Steps = copiedSteps
			return &copied, nil
		}
	}

	return nil, nil
}

func (a *MemorySagaRepositoryAdapter) ReleaseLease(ctx context.Context, tenantID string, id string, fencingToken int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := tenantID + ":" + id
	instance, exists := a.storeByID[key]
	if !exists {
		return errors.NewNotFoundError("saga instance not found")
	}

	if instance.FencingToken != fencingToken {
		return errors.NewConflictError("fencing conflict: stale token cannot release lease")
	}

	instance.LeaseOwner = ""
	instance.LeaseUntil = ""
	instance.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return nil
}

func (a *MemorySagaRepositoryAdapter) AcquireLeaseForSaga(ctx context.Context, tenantID string, id string, workerID string, leaseDuration time.Duration) (*sagaContracts.SagaInstance, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := tenantID + ":" + id
	inst, exists := a.storeByID[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga instance not found")
	}

	now := time.Now().UTC()
	inst.LeaseOwner = workerID
	inst.LeaseUntil = now.Add(leaseDuration).Format(time.RFC3339Nano)
	inst.FencingToken++
	inst.UpdatedAt = now.Format(time.RFC3339Nano)

	copied := *inst
	copiedSteps := make([]sagaContracts.SagaStepExecution, len(inst.Steps))
	copy(copiedSteps, inst.Steps)
	copied.Steps = copiedSteps
	return &copied, nil
}

func (a *MemorySagaRepositoryAdapter) UpdateStepExecution(ctx context.Context, tenantID string, sagaID string, updatedStep *sagaContracts.SagaStepExecution) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := tenantID + ":" + sagaID
	instance, exists := a.storeByID[key]
	if !exists {
		return errors.NewNotFoundError("saga instance not found")
	}

	found := false
	for i := range instance.Steps {
		if instance.Steps[i].StepID == updatedStep.StepID {
			instance.Steps[i] = *updatedStep
			instance.Steps[i].UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			found = true
			break
		}
	}

	if !found {
		return errors.NewNotFoundError("saga step execution not found: " + updatedStep.StepID)
	}
	return nil
}

func (a *MemorySagaRepositoryAdapter) ListSagas(ctx context.Context, tenantID string, limit int, offset int) ([]sagaContracts.SagaInstance, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var result []sagaContracts.SagaInstance
	for _, inst := range a.storeByID {
		if inst.TenantID == tenantID {
			result = append(result, *inst)
		}
	}

	if offset >= len(result) {
		return []sagaContracts.SagaInstance{}, nil
	}

	end := offset + limit
	if end > len(result) {
		end = len(result)
	}

	return result[offset:end], nil
}

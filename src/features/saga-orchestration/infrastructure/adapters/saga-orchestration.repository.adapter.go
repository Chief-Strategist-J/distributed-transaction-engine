package adapters

import (
	"context"
	"sync"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type MemorySagaRepositoryAdapter struct {
	mu    sync.RWMutex
	store map[string]*sagaContracts.SagaInstance
}

func NewMemorySagaRepositoryAdapter() *MemorySagaRepositoryAdapter {
	return &MemorySagaRepositoryAdapter{
		store: make(map[string]*sagaContracts.SagaInstance),
	}
}

func (a *MemorySagaRepositoryAdapter) GetSagaByID(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := tenantID + ":" + id
	instance, exists := a.store[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga instance not found")
	}
	return instance, nil
}

func (a *MemorySagaRepositoryAdapter) CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := instance.TenantID + ":" + instance.ID
	if _, exists := a.store[key]; exists {
		return errors.NewConflictError("saga instance already exists")
	}
	a.store[key] = instance
	return nil
}

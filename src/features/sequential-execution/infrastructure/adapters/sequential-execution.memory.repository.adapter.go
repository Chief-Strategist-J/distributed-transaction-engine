package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"fmt"
	"sync"
)

type MemorySequentialExecutionRepositoryAdapter struct {
	mu      sync.RWMutex
	sagas   map[string]*contracts.SequentialSagaState
	context map[string][]contracts.StepContextEntry
}

func NewMemorySequentialExecutionRepositoryAdapter() *MemorySequentialExecutionRepositoryAdapter {
	return &MemorySequentialExecutionRepositoryAdapter{
		sagas:   make(map[string]*contracts.SequentialSagaState),
		context: make(map[string][]contracts.StepContextEntry),
	}
}

func (m *MemorySequentialExecutionRepositoryAdapter) CreateSequentialSaga(_ context.Context, saga *contracts.SequentialSagaState) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", saga.TenantID, saga.ID)
	if _, exists := m.sagas[key]; exists {
		return errors.NewConflictError("saga already exists")
	}
	cp := *saga
	m.sagas[key] = &cp
	return nil
}

func (m *MemorySequentialExecutionRepositoryAdapter) GetSequentialSaga(_ context.Context, tenantID string, sagaID string) (spanResult0 *contracts.SequentialSagaState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, sagaID)
	s, exists := m.sagas[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga not found")
	}
	cp := *s
	return &cp, nil
}

func (m *MemorySequentialExecutionRepositoryAdapter) UpdateSagaCAS(
	_ context.Context,
	tenantID string,
	sagaID string,
	expectedVersion int64,
	ownerEpoch int64,
	newState string,
	nextStepIndex int,
	nextPhase contracts.ExecutionPhase,
	pivotReached bool,
	output map[string]interface{},
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, sagaID)
	s, exists := m.sagas[key]
	if !exists {
		return errors.NewNotFoundError("saga not found")
	}

	if s.Version != expectedVersion || s.OwnerEpoch != ownerEpoch {
		return errors.NewConflictError("CAS mismatch on version or ownerEpoch")
	}

	s.CurrentState = newState
	s.CurrentStepIndex = nextStepIndex
	s.Phase = nextPhase
	s.PivotReached = pivotReached
	s.FinalOutput = output
	s.Version++
	return nil
}

func (m *MemorySequentialExecutionRepositoryAdapter) SaveContextEntry(_ context.Context, entry *contracts.StepContextEntry) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", entry.TenantID, entry.SagaID)
	m.context[key] = append(m.context[key], *entry)
	return nil
}

func (m *MemorySequentialExecutionRepositoryAdapter) GetContextEntries(_ context.Context, tenantID string, sagaID string) (spanResult0 []contracts.StepContextEntry, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, sagaID)
	entries := m.context[key]
	res := make([]contracts.StepContextEntry, len(entries))
	copy(res, entries)
	return res, nil
}

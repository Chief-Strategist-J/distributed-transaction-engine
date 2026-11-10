package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryOCCRepositoryAdapter struct {
	mu            sync.RWMutex
	sagas         map[string]contracts.SagaRootState
	pendingInputs map[string]contracts.OCCPendingInput
	contention    []contracts.ContentionJournalEntry
	appliedInputs map[string]bool
}

var _ ports.RepositoryPort = (*MemoryOCCRepositoryAdapter)(nil)

func NewMemoryOCCRepositoryAdapter() *MemoryOCCRepositoryAdapter {
	return &MemoryOCCRepositoryAdapter{
		sagas:         make(map[string]contracts.SagaRootState),
		pendingInputs: make(map[string]contracts.OCCPendingInput),
		contention:    make([]contracts.ContentionJournalEntry, 0),
		appliedInputs: make(map[string]bool),
	}
}

func (a *MemoryOCCRepositoryAdapter) SeedSaga(state contracts.SagaRootState) {
	defer observability.TraceEnter(nil)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s", state.TenantID, state.ID.String())
	a.sagas[key] = state
}

func (a *MemoryOCCRepositoryAdapter) ExecuteCAS(ctx context.Context, input contracts.CASMutationInput) (spanResult0 *contracts.CASResult, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", input.TenantID, input.SagaID.String())
	saga, exists := a.sagas[key]
	if !exists {
		return &contracts.CASResult{
			Succeeded:    false,
			Cause:        contracts.CauseDataError,
			ErrorMessage: "saga root aggregate not found",
		}, nil
	}

	if saga.OwnerEpoch != input.ExpectedEpoch {
		return &contracts.CASResult{
			Succeeded:      false,
			Cause:          contracts.CauseNotOwner,
			CurrentVersion: saga.Version,
			CurrentEpoch:   saga.OwnerEpoch,
			CurrentState:   saga.CurrentState,
			ErrorMessage:   "ownership epoch mismatch; worker is not active owner",
		}, nil
	}

	if saga.Version != input.ExpectedVersion {
		return &contracts.CASResult{
			Succeeded:      false,
			Cause:          contracts.CauseVersionConflict,
			CurrentVersion: saga.Version,
			CurrentEpoch:   saga.OwnerEpoch,
			CurrentState:   saga.CurrentState,
			ErrorMessage:   "optimistic concurrency version conflict",
		}, nil
	}

	saga.CurrentState = input.TargetState
	saga.Version++
	saga.UpdatedAt = time.Now().UTC()
	a.sagas[key] = saga

	inputKey := fmt.Sprintf("%s:%s:%s", input.TenantID, input.SagaID.String(), input.InputID)
	a.appliedInputs[inputKey] = true

	return &contracts.CASResult{
		Succeeded:      true,
		CurrentVersion: saga.Version,
		CurrentEpoch:   saga.OwnerEpoch,
		CurrentState:   saga.CurrentState,
	}, nil
}

func (a *MemoryOCCRepositoryAdapter) DiagnoseState(ctx context.Context, tenantID string, sagaID uuid.UUID) (spanResult0 *contracts.SagaRootState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, sagaID.String())
	saga, exists := a.sagas[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("saga root state not found")
	}
	return &saga, nil
}

func (a *MemoryOCCRepositoryAdapter) ParkPendingInput(ctx context.Context, pending contracts.OCCPendingInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", pending.TenantID, pending.ID.String())
	a.pendingInputs[key] = pending
	return nil
}

func (a *MemoryOCCRepositoryAdapter) RecordContention(ctx context.Context, entry contracts.ContentionJournalEntry) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	a.contention = append(a.contention, entry)
	return nil
}

func (a *MemoryOCCRepositoryAdapter) IsInputApplied(ctx context.Context, tenantID string, sagaID uuid.UUID, inputID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, sagaID.String(), inputID)
	return a.appliedInputs[key], nil
}

func (a *MemoryOCCRepositoryAdapter) MarkInputApplied(ctx context.Context, tenantID string, sagaID uuid.UUID, inputID string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, sagaID.String(), inputID)
	a.appliedInputs[key] = true
	return nil
}

func (a *MemoryOCCRepositoryAdapter) GetRecentContentionCount(ctx context.Context, tenantID string, sagaID uuid.UUID, window time.Duration) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-window)
	count := 0
	for _, entry := range a.contention {
		if entry.TenantID == tenantID && entry.SagaID == sagaID && entry.CreatedAt.After(cutoff) {
			count++
		}
	}
	return count, nil
}

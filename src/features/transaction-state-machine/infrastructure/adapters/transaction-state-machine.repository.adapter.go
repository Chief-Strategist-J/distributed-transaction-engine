package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type MemoryTransactionStateMachineRepositoryAdapter struct {
	mu     sync.RWMutex
	states map[string]*contracts.SagaStateRecord
	audits map[string][]contracts.StateTransitionAuditRecord
}

func NewMemoryTransactionStateMachineRepositoryAdapter() *MemoryTransactionStateMachineRepositoryAdapter {
	return &MemoryTransactionStateMachineRepositoryAdapter{
		states: make(map[string]*contracts.SagaStateRecord),
		audits: make(map[string][]contracts.StateTransitionAuditRecord),
	}
}

func (a *MemoryTransactionStateMachineRepositoryAdapter) GetState(ctx context.Context, tenantID string, sagaID string) (*contracts.SagaStateRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := tenantID + ":" + sagaID
	record, exists := a.states[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga state record not found")
	}

	clone := *record
	return &clone, nil
}

func (a *MemoryTransactionStateMachineRepositoryAdapter) CreateState(ctx context.Context, record *contracts.SagaStateRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := record.TenantID + ":" + record.ID
	if _, exists := a.states[key]; exists {
		return errors.NewConflictError("saga state record already exists")
	}

	clone := *record
	a.states[key] = &clone
	return nil
}

func (a *MemoryTransactionStateMachineRepositoryAdapter) UpdateStateCAS(
	ctx context.Context,
	tenantID string,
	sagaID string,
	fromState contracts.TransactionState,
	toState contracts.TransactionState,
	expectedVersion int64,
	fencingToken int64,
	reason string,
) (*contracts.SagaStateRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := tenantID + ":" + sagaID
	current, exists := a.states[key]
	if !exists {
		return nil, errors.NewNotFoundError("saga state record not found")
	}

	if current.Version != expectedVersion {
		return nil, errors.NewConflictError("optimistic concurrency conflict: version mismatch")
	}

	if current.CurrentState != fromState {
		return nil, errors.NewConflictError("state mismatch during CAS update")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	updatedRecord := &contracts.SagaStateRecord{
		ID:            current.ID,
		PublicID:      current.PublicID,
		TenantID:      current.TenantID,
		Source:        current.Source,
		ExternalID:    current.ExternalID,
		CorrelationID: current.CorrelationID,
		CurrentState:  toState,
		Version:       current.Version + 1,
		FencingToken:  fencingToken,
		CreatedAt:     current.CreatedAt,
		UpdatedAt:     now,
	}

	auditPublicID := fmt.Sprintf("aud_00000000000000000000%06d", updatedRecord.Version)
	auditEntry := contracts.StateTransitionAuditRecord{
		ID:            fmt.Sprintf("audit_%s_%d", sagaID, updatedRecord.Version),
		PublicID:      auditPublicID,
		TenantID:      tenantID,
		SagaID:        sagaID,
		CorrelationID: current.CorrelationID,
		FromState:     fromState,
		ToState:       toState,
		VersionBefore: current.Version,
		VersionAfter:  updatedRecord.Version,
		FencingToken:  fencingToken,
		Reason:        reason,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	a.states[key] = updatedRecord
	a.audits[key] = append(a.audits[key], auditEntry)

	clone := *updatedRecord
	return &clone, nil
}

func (a *MemoryTransactionStateMachineRepositoryAdapter) GetTransitionAuditLogs(ctx context.Context, tenantID string, sagaID string) ([]contracts.StateTransitionAuditRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := tenantID + ":" + sagaID
	records, exists := a.audits[key]
	if !exists {
		return []contracts.StateTransitionAuditRecord{}, nil
	}

	copied := make([]contracts.StateTransitionAuditRecord, len(records))
	copy(copied, records)
	return copied, nil
}

package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryCompensationRetryRepositoryAdapter struct {
	mu              sync.RWMutex
	records         map[string]contracts.CompensationRetryRecord
	pausedStepTypes map[string]bool
}

var _ ports.RepositoryPort = (*MemoryCompensationRetryRepositoryAdapter)(nil)

func NewMemoryCompensationRetryRepositoryAdapter() *MemoryCompensationRetryRepositoryAdapter {
	return &MemoryCompensationRetryRepositoryAdapter{
		records:         make(map[string]contracts.CompensationRetryRecord),
		pausedStepTypes: make(map[string]bool),
	}
}

func (a *MemoryCompensationRetryRepositoryAdapter) UpsertAttempt(ctx context.Context, record contracts.CompensationRetryRecord) (spanResult0 *contracts.CompensationRetryRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", record.TenantID, record.SagaID.String(), record.StepID)
	now := time.Now().UTC()

	existing, exists := a.records[key]
	if exists {
		existing.AttemptNumber = record.AttemptNumber
		existing.NextRetryAt = record.NextRetryAt
		existing.Status = record.Status
		existing.LastError = record.LastError
		existing.Version++
		existing.UpdatedAt = now
		a.records[key] = existing
		return &existing, nil
	}

	record.CreatedAt = now
	record.UpdatedAt = now
	record.Version = 1
	a.records[key] = record
	return &record, nil
}

func (a *MemoryCompensationRetryRepositoryAdapter) GetRecord(ctx context.Context, tenantID string, sagaID uuid.UUID, stepID string) (spanResult0 *contracts.CompensationRetryRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, sagaID.String(), stepID)
	record, exists := a.records[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("compensation retry record not found")
	}
	return &record, nil
}

func (a *MemoryCompensationRetryRepositoryAdapter) UpdateStatus(ctx context.Context, tenantID string, sagaID uuid.UUID, stepID string, status string, nextRetryAt *time.Time, lastError string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, sagaID.String(), stepID)
	record, exists := a.records[key]
	if !exists {
		return appErrors.NewNotFoundError("compensation retry record not found")
	}

	record.Status = status
	record.NextRetryAt = nextRetryAt
	record.LastError = lastError
	record.Version++
	record.UpdatedAt = time.Now().UTC()
	a.records[key] = record
	return nil
}

func (a *MemoryCompensationRetryRepositoryAdapter) PauseStepType(ctx context.Context, tenantID string, stepType string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, stepType)
	a.pausedStepTypes[key] = true
	return nil
}

func (a *MemoryCompensationRetryRepositoryAdapter) ResumeStepType(ctx context.Context, tenantID string, stepType string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, stepType)
	delete(a.pausedStepTypes, key)
	return nil
}

func (a *MemoryCompensationRetryRepositoryAdapter) IsStepTypePaused(ctx context.Context, tenantID string, stepType string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, stepType)
	return a.pausedStepTypes[key], nil
}

package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/features/idempotency-keys/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	"fmt"
	"sync"
	"time"
)

type MemoryIdempotencyRepositoryAdapter struct {
	mu      sync.Mutex
	records map[string]contracts.IdempotencyRecord
}

func NewMemoryIdempotencyRepositoryAdapter() *MemoryIdempotencyRepositoryAdapter {
	return &MemoryIdempotencyRepositoryAdapter{
		records: make(map[string]contracts.IdempotencyRecord),
	}
}

func (a *MemoryIdempotencyRepositoryAdapter) scopeKey(tenantID, principalID, operationID, idempotencyKey string) string {
	return fmt.Sprintf("%s:%s:%s:%s", tenantID, principalID, operationID, idempotencyKey)
}

func (a *MemoryIdempotencyRepositoryAdapter) Acquire(
	ctx context.Context,
	record contracts.IdempotencyRecord,
) (spanResult0 *contracts.AcquireResult, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	k := a.scopeKey(record.TenantID, record.PrincipalID, record.OperationID, record.IdempotencyKey)
	existing, exists := a.records[k]
	now := time.Now().UTC()

	if !exists {
		record.CreatedAt = now
		record.UpdatedAt = now
		a.records[k] = record
		recCopy := record
		return &contracts.AcquireResult{
			Outcome: contracts.OutcomeAcquired,
			Record:  &recCopy,
		}, nil
	}

	outcome := rules.EvaluateExistingRecord(&existing, record.RequestFingerprint, now)
	if outcome == contracts.OutcomeAcquired {
		record.CreatedAt = existing.CreatedAt
		record.UpdatedAt = now
		a.records[k] = record
		recCopy := record
		return &contracts.AcquireResult{
			Outcome: contracts.OutcomeAcquired,
			Record:  &recCopy,
		}, nil
	}

	recCopy := existing
	var snapshot *contracts.ResponseSnapshot
	if existing.ResponseStatus != nil {
		snapshot = &contracts.ResponseSnapshot{
			StatusCode: *existing.ResponseStatus,
			Headers:    existing.ResponseHeaders,
			Body:       existing.ResponseBody,
		}
	}

	return &contracts.AcquireResult{
		Outcome:  outcome,
		Record:   &recCopy,
		Snapshot: snapshot,
	}, nil
}

func (a *MemoryIdempotencyRepositoryAdapter) GetByScope(
	ctx context.Context,
	tenantID, principalID, operationID, idempotencyKey string,
) (spanResult0 *contracts.IdempotencyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	k := a.scopeKey(tenantID, principalID, operationID, idempotencyKey)
	rec, exists := a.records[k]
	if !exists {
		return nil, nil
	}
	recCopy := rec
	return &recCopy, nil
}

func (a *MemoryIdempotencyRepositoryAdapter) CommitOutcome(
	ctx context.Context,
	tenantID, id string,
	state contracts.IdempotencyState,
	snapshot contracts.ResponseSnapshot,
	resourceRef string,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now().UTC()
	for k, rec := range a.records {
		if rec.TenantID == tenantID && rec.ID.String() == id {
			rec.CurrentState = state
			rec.ResponseStatus = &snapshot.StatusCode
			rec.ResponseHeaders = snapshot.Headers
			rec.ResponseBody = snapshot.Body
			rec.ResourceReference = resourceRef
			rec.CompletedAt = &now
			rec.UpdatedAt = now
			a.records[k] = rec
			return nil
		}
	}
	return nil
}

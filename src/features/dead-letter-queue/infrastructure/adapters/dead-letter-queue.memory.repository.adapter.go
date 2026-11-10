package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryDeadLetterRepositoryAdapter struct {
	mu           sync.RWMutex
	records      map[string]contracts.DeadLetterRecord
	attempts     map[string]bool
	incidents    map[string]contracts.SystemicIncident
	pausedSteps  map[string]contracts.StepTypePause
	outboxEvents []string
}

var _ ports.RepositoryPort = (*MemoryDeadLetterRepositoryAdapter)(nil)

func NewMemoryDeadLetterRepositoryAdapter() *MemoryDeadLetterRepositoryAdapter {
	return &MemoryDeadLetterRepositoryAdapter{
		records:      make(map[string]contracts.DeadLetterRecord),
		attempts:     make(map[string]bool),
		incidents:    make(map[string]contracts.SystemicIncident),
		pausedSteps:  make(map[string]contracts.StepTypePause),
		outboxEvents: make([]string, 0),
	}
}

func (a *MemoryDeadLetterRepositoryAdapter) AdmitAtomic(
	ctx context.Context,
	record contracts.DeadLetterRecord,
	outboxPayload string,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	attemptKey := fmt.Sprintf("%s:%s:%d:%s", record.TenantID, record.SagaID.String(), record.StepIndex, record.AttemptID.String())
	if a.attempts[attemptKey] {
		return nil, appErrors.NewConflictError("attempt already dead-lettered")
	}

	recKey := fmt.Sprintf("%s:%s", record.TenantID, record.ID.String())
	now := time.Now().UTC()
	record.CreatedAt = now
	record.UpdatedAt = now
	record.Version = 1

	a.records[recKey] = record
	a.attempts[attemptKey] = true
	if outboxPayload != "" {
		a.outboxEvents = append(a.outboxEvents, outboxPayload)
	}

	return &record, nil
}

func (a *MemoryDeadLetterRepositoryAdapter) GetRecordByID(
	ctx context.Context,
	tenantID string,
	id uuid.UUID,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	recKey := fmt.Sprintf("%s:%s", tenantID, id.String())
	rec, exists := a.records[recKey]
	if !exists {
		return nil, appErrors.NewNotFoundError("dead letter record not found")
	}
	return &rec, nil
}

func (a *MemoryDeadLetterRepositoryAdapter) UpdateRecordStatus(
	ctx context.Context,
	tenantID string,
	id uuid.UUID,
	expectedVersion int64,
	newStatus contracts.RecordStatus,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	recKey := fmt.Sprintf("%s:%s", tenantID, id.String())
	rec, exists := a.records[recKey]
	if !exists {
		return nil, appErrors.NewNotFoundError("dead letter record not found")
	}
	if rec.Version != expectedVersion {
		return nil, appErrors.NewPreconditionFailedError("optimistic concurrency conflict on dead letter record")
	}

	rec.Status = newStatus
	rec.Version++
	rec.UpdatedAt = time.Now().UTC()
	a.records[recKey] = rec
	return &rec, nil
}

func (a *MemoryDeadLetterRepositoryAdapter) ResolveRecord(
	ctx context.Context,
	tenantID string,
	id uuid.UUID,
	expectedVersion int64,
	disposition contracts.DispositionType,
	actorID string,
	secondApproverID string,
	reason string,
	evidenceRef string,
	correctiveAttemptID *uuid.UUID,
	correctiveIdempotencyKey string,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	recKey := fmt.Sprintf("%s:%s", tenantID, id.String())
	rec, exists := a.records[recKey]
	if !exists {
		return nil, appErrors.NewNotFoundError("dead letter record not found")
	}
	if rec.Version != expectedVersion {
		return nil, appErrors.NewPreconditionFailedError("optimistic concurrency conflict on dead letter record")
	}

	now := time.Now().UTC()
	rec.Status = contracts.StatusResolved
	rec.Disposition = &disposition
	rec.ActorID = &actorID
	if secondApproverID != "" {
		rec.SecondApproverID = &secondApproverID
	}
	if reason != "" {
		rec.DispositionReason = &reason
	}
	if evidenceRef != "" {
		rec.EvidenceRef = &evidenceRef
	}
	if correctiveAttemptID != nil {
		rec.CorrectiveAttemptID = correctiveAttemptID
	}
	if correctiveIdempotencyKey != "" {
		rec.CorrectiveIdempotencyKey = &correctiveIdempotencyKey
	}
	rec.Version++
	rec.UpdatedAt = now

	a.records[recKey] = rec
	return &rec, nil
}

func (a *MemoryDeadLetterRepositoryAdapter) CountRecentByFingerprint(
	ctx context.Context,
	tenantID string,
	fingerprint string,
	window time.Duration,
) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-window)
	count := 0
	for _, rec := range a.records {
		if rec.TenantID == tenantID && rec.ErrorFingerprint == fingerprint && rec.CreatedAt.After(cutoff) {
			count++
		}
	}
	return count, nil
}

func (a *MemoryDeadLetterRepositoryAdapter) CreateSystemicIncident(
	ctx context.Context,
	incident contracts.SystemicIncident,
	pause contracts.StepTypePause,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	incKey := fmt.Sprintf("%s:%s", incident.TenantID, incident.ID.String())
	a.incidents[incKey] = incident

	pauseKey := fmt.Sprintf("%s:%s", pause.TenantID, pause.StepType)
	a.pausedSteps[pauseKey] = pause
	return nil
}

func (a *MemoryDeadLetterRepositoryAdapter) IsStepTypePaused(
	ctx context.Context,
	tenantID string,
	stepType string,
) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	pauseKey := fmt.Sprintf("%s:%s", tenantID, stepType)
	_, paused := a.pausedSteps[pauseKey]
	return paused, nil
}

func (a *MemoryDeadLetterRepositoryAdapter) ListRecords(
	ctx context.Context,
	tenantID string,
	status *contracts.RecordStatus,
	limit int,
) (spanResult0 []contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make([]contracts.DeadLetterRecord, 0)
	for _, rec := range a.records {
		if rec.TenantID != tenantID {
			continue
		}
		if status != nil && rec.Status != *status {
			continue
		}
		result = append(result, rec)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"distributed-transaction-engine/src/features/audit-trail/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"sync"
	"time"
)

type AuditTrailMemoryRepositoryAdapter struct {
	mu            sync.RWMutex
	outbox        []contracts.OutboxEventRecord
	ledger        []contracts.AuditEventRecord
	quarantine    []contracts.AuditQuarantineRecord
	checkpoints   []contracts.AuditCheckpointRecord
	verifications []contracts.AuditVerificationRecord
}

func NewAuditTrailMemoryRepositoryAdapter() *AuditTrailMemoryRepositoryAdapter {
	return &AuditTrailMemoryRepositoryAdapter{
		outbox:        make([]contracts.OutboxEventRecord, 0),
		ledger:        make([]contracts.AuditEventRecord, 0),
		quarantine:    make([]contracts.AuditQuarantineRecord, 0),
		checkpoints:   make([]contracts.AuditCheckpointRecord, 0),
		verifications: make([]contracts.AuditVerificationRecord, 0),
	}
}

func (a *AuditTrailMemoryRepositoryAdapter) AppendOutbox(ctx context.Context, record *contracts.OutboxEventRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.outbox = append(a.outbox, *record)
	return nil
}

func (a *AuditTrailMemoryRepositoryAdapter) GetPendingOutbox(ctx context.Context, tenantID string, limit int) (spanResult0 []contracts.OutboxEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var results []contracts.OutboxEventRecord
	for _, item := range a.outbox {
		if item.TenantID == tenantID && item.Status == contracts.OutboxStatusPending {
			results = append(results, item)
			if len(results) >= limit {
				break
			}
		}
	}
	return results, nil
}

func (a *AuditTrailMemoryRepositoryAdapter) MarkOutboxChained(ctx context.Context, tenantID string, ids []uuid.UUID, chainedAt time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	idMap := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		idMap[id] = struct{}{}
	}
	for i := range a.outbox {
		if a.outbox[i].TenantID == tenantID {
			if _, exists := idMap[a.outbox[i].ID]; exists {
				a.outbox[i].Status = contracts.OutboxStatusChained
				a.outbox[i].ChainedAt = &chainedAt
			}
		}
	}
	return nil
}

func (a *AuditTrailMemoryRepositoryAdapter) GetTailEvent(ctx context.Context, tenantID string, segmentID uuid.UUID) (spanResult0 *contracts.AuditEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var tail *contracts.AuditEventRecord
	for i := range a.ledger {
		if a.ledger[i].TenantID == tenantID && a.ledger[i].SegmentID == segmentID {
			if tail == nil || a.ledger[i].SequenceID > tail.SequenceID {
				rec := a.ledger[i]
				tail = &rec
			}
		}
	}
	return tail, nil
}

func (a *AuditTrailMemoryRepositoryAdapter) AppendChainedEvent(ctx context.Context, record *contracts.AuditEventRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ledger = append(a.ledger, *record)
	return nil
}

func (a *AuditTrailMemoryRepositoryAdapter) GetEventsBySegment(ctx context.Context, tenantID string, segmentID uuid.UUID, limit int) (spanResult0 []contracts.AuditEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var results []contracts.AuditEventRecord
	for _, item := range a.ledger {
		if item.TenantID == tenantID && item.SegmentID == segmentID {
			results = append(results, item)
			if len(results) >= limit {
				break
			}
		}
	}
	return results, nil
}

func (a *AuditTrailMemoryRepositoryAdapter) GetEventsByTenant(ctx context.Context, query contracts.ListAuditEventsQuery) (spanResult0 []contracts.AuditEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var results []contracts.AuditEventRecord
	for i := len(a.ledger) - 1; i >= 0; i-- {
		item := a.ledger[i]
		if item.TenantID != query.TenantID {
			continue
		}
		if query.ActorID != "" && item.ActorID != query.ActorID {
			continue
		}
		if query.Action != "" && item.Action != query.Action {
			continue
		}
		if query.MaskPII {
			item.RawPayload = rules.MaskSensitivePayload(item.RawPayload)
		}
		results = append(results, item)
		if len(results) >= query.Limit {
			break
		}
	}
	return results, nil
}

func (a *AuditTrailMemoryRepositoryAdapter) IsAuditIDChained(ctx context.Context, tenantID string, auditID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, item := range a.ledger {
		if item.TenantID == tenantID && item.AuditID == auditID {
			return true, nil
		}
	}
	return false, nil
}

func (a *AuditTrailMemoryRepositoryAdapter) AppendQuarantine(ctx context.Context, record *contracts.AuditQuarantineRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.quarantine = append(a.quarantine, *record)
	return nil
}

func (a *AuditTrailMemoryRepositoryAdapter) GetQuarantinedEvents(ctx context.Context, tenantID string, limit int) (spanResult0 []contracts.AuditQuarantineRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var results []contracts.AuditQuarantineRecord
	for i := len(a.quarantine) - 1; i >= 0; i-- {
		if a.quarantine[i].TenantID == tenantID {
			results = append(results, a.quarantine[i])
			if len(results) >= limit {
				break
			}
		}
	}
	return results, nil
}

func (a *AuditTrailMemoryRepositoryAdapter) SaveCheckpoint(ctx context.Context, record *contracts.AuditCheckpointRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.checkpoints = append(a.checkpoints, *record)
	return nil
}

func (a *AuditTrailMemoryRepositoryAdapter) GetLatestCheckpoint(ctx context.Context, tenantID string, segmentID uuid.UUID) (spanResult0 *contracts.AuditCheckpointRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var latest *contracts.AuditCheckpointRecord
	for i := range a.checkpoints {
		if a.checkpoints[i].TenantID == tenantID && a.checkpoints[i].SegmentID == segmentID {
			if latest == nil || a.checkpoints[i].CreatedAt.After(latest.CreatedAt) {
				cp := a.checkpoints[i]
				latest = &cp
			}
		}
	}
	return latest, nil
}

func (a *AuditTrailMemoryRepositoryAdapter) RecordVerification(ctx context.Context, record *contracts.AuditVerificationRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.verifications = append(a.verifications, *record)
	return nil
}

package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryTransactionQueryAdapter struct {
	mu           sync.RWMutex
	transactions map[string]contracts.Row
	steps        map[string][]contracts.TimelineRow
	watermarks   map[contracts.StoreName]contracts.Watermark
	savedQueries map[string]contracts.SavedQuery
	exportJobs   map[string]contracts.ExportJob
	exportChunks map[string][]contracts.ExportChunk
	audits       []contracts.AuditRecord
}

func NewMemoryTransactionQueryAdapter() *MemoryTransactionQueryAdapter {
	now := time.Now().UTC()
	return &MemoryTransactionQueryAdapter{
		transactions: make(map[string]contracts.Row),
		steps:        make(map[string][]contracts.TimelineRow),
		watermarks: map[contracts.StoreName]contracts.Watermark{
			contracts.StorePrimary: {Store: contracts.StorePrimary, Sequence: 1, AsOf: now},
			contracts.StoreSearch:  {Store: contracts.StoreSearch, Sequence: 1, AsOf: now},
			contracts.StoreHistory: {Store: contracts.StoreHistory, Sequence: 1, AsOf: now},
		},
		savedQueries: make(map[string]contracts.SavedQuery),
		exportJobs:   make(map[string]contracts.ExportJob),
		exportChunks: make(map[string][]contracts.ExportChunk),
		audits:       make([]contracts.AuditRecord, 0),
	}
}

func (m *MemoryTransactionQueryAdapter) FindTransactionByID(ctx context.Context, tenantID string, publicID string, injected contracts.InjectedPredicates, fields []string) (spanResult0 contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tenantID + ":" + publicID
	row, ok := m.transactions[key]
	if !ok {
		return nil, appErrors.NewNotFoundError("transaction not found")
	}
	return row, nil
}

func (m *MemoryTransactionQueryAdapter) ListTransactionsByIDs(ctx context.Context, tenantID string, publicIDs []string, injected contracts.InjectedPredicates, fields []string) (spanResult0 []contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	results := make([]contracts.Row, 0, len(publicIDs))
	for _, pid := range publicIDs {
		key := tenantID + ":" + pid
		if row, ok := m.transactions[key]; ok {
			results = append(results, row)
		}
	}
	return results, nil
}

func (m *MemoryTransactionQueryAdapter) ListTransactions(ctx context.Context, plan contracts.QueryPlan) (spanResult0 []contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	results := make([]contracts.Row, 0)
	for _, row := range m.transactions {
		if tid, ok := row["tenant_id"].(string); ok && tid == plan.Injected.TenantID {
			results = append(results, row)
		}
	}
	if len(results) > plan.Limit {
		results = results[:plan.Limit]
	}
	return results, nil
}

func (m *MemoryTransactionQueryAdapter) AggregateTransactions(ctx context.Context, plan contracts.AggregatePlan) (spanResult0 []contracts.AggregateGroup, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	var count int64 = 0
	for _, row := range m.transactions {
		if tid, ok := row["tenant_id"].(string); ok && tid == plan.Injected.TenantID {
			count++
		}
	}
	return []contracts.AggregateGroup{
		{
			Keys: map[string]any{"status": "COMPLETED"},
			Metrics: map[string]any{
				"count": count,
			},
		},
	}, nil
}

func (m *MemoryTransactionQueryAdapter) EstimatePlanRows(ctx context.Context, plan contracts.QueryPlan) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.transactions)), nil
}

func (m *MemoryTransactionQueryAdapter) CountTransactions(ctx context.Context, plan contracts.QueryPlan) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.transactions)), nil
}

func (m *MemoryTransactionQueryAdapter) FindTenantRowEstimate(ctx context.Context, tenantID string) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	var count int64 = 0
	for _, row := range m.transactions {
		if tid, ok := row["tenant_id"].(string); ok && tid == tenantID {
			count++
		}
	}
	return count, nil
}

func (m *MemoryTransactionQueryAdapter) FindWatermark(ctx context.Context, store contracts.StoreName) (spanResult0 contracts.Watermark, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	wm, ok := m.watermarks[store]
	if !ok {
		return contracts.Watermark{Store: store, Sequence: 1, AsOf: time.Now().UTC()}, nil
	}
	return wm, nil
}

func (m *MemoryTransactionQueryAdapter) FindTransactionSagaID(ctx context.Context, tenantID string, publicID string, injected contracts.InjectedPredicates) (spanResult0 *uuid.UUID, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tenantID + ":" + publicID
	row, ok := m.transactions[key]
	if !ok {
		return nil, appErrors.NewNotFoundError("transaction not found")
	}
	if sidStr, ok := row["saga_id"].(string); ok {
		parsed, err := uuid.Parse(sidStr)
		if err == nil {
			return &parsed, nil
		}
	}
	newID := uuid.New()
	return &newID, nil
}

func (m *MemoryTransactionQueryAdapter) UpdateTransactionProjection(ctx context.Context, record contracts.ProjectionRecord) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := record.TenantID + ":" + record.PublicID
	row := contracts.Row{
		"id":                  record.PublicID,
		"tenant_id":           record.TenantID,
		"public_id":           record.PublicID,
		"correlation_id":      record.CorrelationID.String(),
		"saga_id":             record.SagaID.String(),
		"internal_status":     record.InternalStatus,
		"status":              record.BusinessStatus,
		"compensation_status": record.CompensationStatus,
		"workflow_type":       record.WorkflowType,
		"workflow_version":    record.WorkflowVersion,
		"amount":              record.AmountMinor,
		"currency":            record.Currency,
		"customer_ref":        record.CustomerRef,
		"created_at":          record.CreatedAt,
		"effective_at":        record.EffectiveAt,
	}
	m.transactions[key] = row
	return 1, nil
}

func (m *MemoryTransactionQueryAdapter) ListSearchMatches(ctx context.Context, plan contracts.QueryPlan) (spanResult0 []contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return m.ListTransactions(ctx, plan)
}

func (m *MemoryTransactionQueryAdapter) AggregateSearchCounts(ctx context.Context, plan contracts.AggregatePlan) (spanResult0 []contracts.AggregateGroup, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return m.AggregateTransactions(ctx, plan)
}

func (m *MemoryTransactionQueryAdapter) FindSearchWatermark(ctx context.Context) (spanResult0 contracts.Watermark, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return m.FindWatermark(ctx, contracts.StoreSearch)
}

func (m *MemoryTransactionQueryAdapter) UpdateSearchDocument(ctx context.Context, record contracts.ProjectionRecord) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return m.UpdateTransactionProjection(ctx, record)
}

func (m *MemoryTransactionQueryAdapter) ListTimelineEvents(ctx context.Context, tenantID string, sagaID uuid.UUID, after *contracts.TimelinePosition, limit int) (spanResult0 []contracts.TimelineRow, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tenantID + ":" + sagaID.String()
	items, ok := m.steps[key]
	if !ok {
		return []contracts.TimelineRow{
			{
				Event: contracts.TimelineEvent{
					StepName:      "payment",
					State:         "COMPLETED",
					BusinessState: "COMPLETED",
					Attempts:      1,
				},
				CreatedAt: time.Now().UTC(),
				StepID:    "step-1",
			},
		}, nil
	}
	return items, nil
}

func (m *MemoryTransactionQueryAdapter) AddTimelineEvent(tenantID string, sagaID uuid.UUID, event contracts.TimelineRow) {
	defer observability.TraceEnter(nil)()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tenantID + ":" + sagaID.String()
	m.steps[key] = append(m.steps[key], event)
}

func (m *MemoryTransactionQueryAdapter) CreateAuditRecord(ctx context.Context, record contracts.AuditRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audits = append(m.audits, record)
	return nil
}

func (m *MemoryTransactionQueryAdapter) UpdateRateBucketTokens(ctx context.Context, tenantID string, bucketKey string, cost int, capacity int, refillPerSecond int) (spanResult0 contracts.RateDecision, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return contracts.RateDecision{Allowed: true, RetryAfterSeconds: 0}, nil
}

func (m *MemoryTransactionQueryAdapter) CreateSavedQuery(ctx context.Context, record contracts.SavedQuery) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := record.TenantID + ":" + record.PublicID
	m.savedQueries[key] = record
	return nil
}

func (m *MemoryTransactionQueryAdapter) FindSavedQuery(ctx context.Context, tenantID string, publicID string) (spanResult0 *contracts.SavedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tenantID + ":" + publicID
	sq, ok := m.savedQueries[key]
	if !ok {
		return nil, appErrors.NewNotFoundError("saved query not found")
	}
	return &sq, nil
}

func (m *MemoryTransactionQueryAdapter) ListSavedQueries(ctx context.Context, tenantID string, limit int) (spanResult0 []contracts.SavedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	results := make([]contracts.SavedQuery, 0, len(m.savedQueries))
	for _, sq := range m.savedQueries {
		if sq.TenantID == tenantID {
			results = append(results, sq)
		}
	}
	return results, nil
}

func (m *MemoryTransactionQueryAdapter) UpdateSavedQueryStatus(ctx context.Context, record contracts.SavedQuery, expectedVersion int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := record.TenantID + ":" + record.PublicID
	m.savedQueries[key] = record
	return nil
}

func (m *MemoryTransactionQueryAdapter) CreateExportJob(ctx context.Context, job contracts.ExportJob) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := job.TenantID + ":" + job.PublicID
	m.exportJobs[key] = job
	return nil
}

func (m *MemoryTransactionQueryAdapter) FindExportJob(ctx context.Context, tenantID string, publicID string) (spanResult0 *contracts.ExportJob, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tenantID + ":" + publicID
	job, ok := m.exportJobs[key]
	if !ok {
		return nil, appErrors.NewNotFoundError("export job not found")
	}
	return &job, nil
}

func (m *MemoryTransactionQueryAdapter) UpdateExportJob(ctx context.Context, job contracts.ExportJob, expectedVersion int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := job.TenantID + ":" + job.PublicID
	m.exportJobs[key] = job
	return nil
}

func (m *MemoryTransactionQueryAdapter) CreateExportChunk(ctx context.Context, tenantID string, chunk contracts.ExportChunk) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tenantID + ":" + chunk.JobID.String()
	m.exportChunks[key] = append(m.exportChunks[key], chunk)
	return nil
}

func (m *MemoryTransactionQueryAdapter) ListExportChunks(ctx context.Context, tenantID string, jobID uuid.UUID) (spanResult0 []contracts.ExportChunk, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tenantID + ":" + jobID.String()
	return m.exportChunks[key], nil
}

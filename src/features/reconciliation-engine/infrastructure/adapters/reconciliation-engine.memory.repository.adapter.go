package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryReconciliationRepositoryAdapter struct {
	mu        sync.RWMutex
	anomalies map[string]contracts.ReconciliationAnomalyRecord
}

func NewMemoryReconciliationRepositoryAdapter() *MemoryReconciliationRepositoryAdapter {
	return &MemoryReconciliationRepositoryAdapter{
		anomalies: make(map[string]contracts.ReconciliationAnomalyRecord),
	}
}

func (a *MemoryReconciliationRepositoryAdapter) SaveAnomaly(ctx context.Context, anomaly contracts.ReconciliationAnomalyRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := anomaly.TenantID + ":" + anomaly.SagaID.String()
	a.anomalies[key] = anomaly
	return nil
}

func (a *MemoryReconciliationRepositoryAdapter) GetAnomalyBySaga(ctx context.Context, tenantID string, sagaID uuid.UUID) (spanResult0 *contracts.ReconciliationAnomalyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := tenantID + ":" + sagaID.String()
	record, exists := a.anomalies[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("Anomaly not found for saga")
	}
	return &record, nil
}

func (a *MemoryReconciliationRepositoryAdapter) ListAnomalies(ctx context.Context, tenantID string, status *contracts.ReconciliationStatus) (spanResult0 []contracts.ReconciliationAnomalyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	var result []contracts.ReconciliationAnomalyRecord
	for _, rec := range a.anomalies {
		if rec.TenantID != tenantID {
			continue
		}
		if status != nil && rec.Status != *status {
			continue
		}
		result = append(result, rec)
	}
	return result, nil
}

func (a *MemoryReconciliationRepositoryAdapter) UpdateAnomalyStatus(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, nextStatus contracts.ReconciliationStatus, resolutionAction string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, rec := range a.anomalies {
		if rec.TenantID == tenantID && rec.ID == id {
			if rec.Version != expectedVersion {
				return appErrors.NewConflictError("Version conflict in reconciliation anomaly")
			}
			rec.Status = nextStatus
			rec.ResolutionAction = resolutionAction
			rec.Version++
			rec.UpdatedAt = time.Now().UTC()
			a.anomalies[k] = rec
			return nil
		}
	}
	return appErrors.NewNotFoundError("Anomaly record not found")
}

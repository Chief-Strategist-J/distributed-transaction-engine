package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"sync"
)

type MemoryManualInterventionRepositoryAdapter struct {
	mu      sync.RWMutex
	records []contracts.ManualInterventionAuditRecord
}

func NewMemoryManualInterventionRepositoryAdapter() *MemoryManualInterventionRepositoryAdapter {
	return &MemoryManualInterventionRepositoryAdapter{
		records: make([]contracts.ManualInterventionAuditRecord, 0),
	}
}

func (a *MemoryManualInterventionRepositoryAdapter) SaveAuditRecord(ctx context.Context, record contracts.ManualInterventionAuditRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	a.records = append(a.records, record)
	return nil
}

func (a *MemoryManualInterventionRepositoryAdapter) ListAuditRecordsBySaga(ctx context.Context, tenantID string, sagaID uuid.UUID) (spanResult0 []contracts.ManualInterventionAuditRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	var result []contracts.ManualInterventionAuditRecord
	for _, rec := range a.records {
		if rec.TenantID == tenantID && rec.SagaID == sagaID {
			result = append(result, rec)
		}
	}
	return result, nil
}

func (a *MemoryManualInterventionRepositoryAdapter) ListAuditRecordsByOperator(ctx context.Context, tenantID string, operatorID string) (spanResult0 []contracts.ManualInterventionAuditRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	var result []contracts.ManualInterventionAuditRecord
	for _, rec := range a.records {
		if rec.TenantID == tenantID && rec.OperatorID == operatorID {
			result = append(result, rec)
		}
	}
	return result, nil
}

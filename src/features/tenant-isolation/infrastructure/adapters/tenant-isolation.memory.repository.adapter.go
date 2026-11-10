package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/machines"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type MemoryTenantIsolationRepositoryAdapter struct {
	mu          sync.Mutex
	tenants     map[string]contracts.TenantRecord
	tombstones  map[string]struct{}
	quotas      map[string]contracts.SagaQuotaRecord
	quarantine  map[string]contracts.QuarantineRecord
	securityLog []contracts.SecurityEventRecord
}

func NewMemoryTenantIsolationRepositoryAdapter() *MemoryTenantIsolationRepositoryAdapter {
	return &MemoryTenantIsolationRepositoryAdapter{
		tenants:    make(map[string]contracts.TenantRecord),
		tombstones: make(map[string]struct{}),
		quotas:     make(map[string]contracts.SagaQuotaRecord),
		quarantine: make(map[string]contracts.QuarantineRecord),
	}
}

func (a *MemoryTenantIsolationRepositoryAdapter) CreateTenant(ctx context.Context, record contracts.TenantRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, tombstoned := a.tombstones[record.TenantID]; tombstoned {
		return appErrors.NewConflictError("tenant id was offboarded and can never be reissued")
	}
	if _, exists := a.tenants[record.TenantID]; exists {
		return appErrors.NewConflictError("record already exists")
	}
	a.tenants[record.TenantID] = record
	a.quotas[record.TenantID] = contracts.SagaQuotaRecord{TenantID: record.TenantID, Version: 1, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt}
	return nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) FindTenantByTenantID(ctx context.Context, tenantID string) (spanResult0 *contracts.TenantRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	record, exists := a.tenants[tenantID]
	if !exists {
		return nil, nil
	}
	return &record, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) UpdateTenantStatus(ctx context.Context, input contracts.UpdateTenantStatusInput) (spanResult0 contracts.TenantRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	record, exists := a.tenants[input.TenantID]
	if !exists || record.Version != input.ExpectedVersion {
		return contracts.TenantRecord{}, appErrors.NewPreconditionFailedError("tenant version does not match the expected version")
	}
	if !machines.IsTenantStatusTransitionAllowed(record.Status, input.TargetStatus) {
		return contracts.TenantRecord{}, appErrors.NewConflictError("tenant status transition is not allowed")
	}
	record.Status, record.SuspensionReason = input.TargetStatus, input.SuspensionReason
	record.Version, record.UpdatedAt, record.StatusChangedAt = record.Version+1, time.Now().UTC(), time.Now().UTC()
	a.tenants[input.TenantID] = record
	if record.Status == contracts.TenantStatusOffboarded {
		a.tombstones[record.TenantID] = struct{}{}
	}
	return record, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) FindSagaQuota(ctx context.Context, tenantID string) (spanResult0 *contracts.SagaQuotaRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	record, exists := a.quotas[tenantID]
	if !exists {
		return nil, nil
	}
	return &record, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) CreateSagaSlotReservation(ctx context.Context, tenantID string) (spanResult0 *contracts.SagaQuotaReservation, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	tenant, tenantExists := a.tenants[tenantID]
	quota, quotaExists := a.quotas[tenantID]
	if !tenantExists || !quotaExists || tenant.Status != contracts.TenantStatusActive || quota.ActiveSagaCount >= int64(tenant.MaxConcurrentSagas) {
		return nil, nil
	}
	quota.ActiveSagaCount, quota.Version, quota.UpdatedAt = quota.ActiveSagaCount+1, quota.Version+1, time.Now().UTC()
	a.quotas[tenantID] = quota
	return &contracts.SagaQuotaReservation{TenantID: tenantID, ActiveSagaCount: quota.ActiveSagaCount, MaxConcurrentSagas: tenant.MaxConcurrentSagas}, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) DeleteSagaSlotReservation(ctx context.Context, tenantID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	quota, exists := a.quotas[tenantID]
	if !exists || quota.ActiveSagaCount <= 0 {
		return false, nil
	}
	quota.ActiveSagaCount, quota.Version, quota.UpdatedAt = quota.ActiveSagaCount-1, quota.Version+1, time.Now().UTC()
	a.quotas[tenantID] = quota
	return true, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) UpdateSagaQuotaCounter(ctx context.Context, input contracts.UpdateSagaQuotaCounterInput) (spanResult0 contracts.SagaQuotaRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	quota, exists := a.quotas[input.TenantID]
	if !exists || quota.Version != input.ExpectedVersion {
		return contracts.SagaQuotaRecord{}, appErrors.NewPreconditionFailedError("quota version does not match the expected version")
	}
	quota.ActiveSagaCount, quota.Version, quota.UpdatedAt = input.ObservedActiveSagas, quota.Version+1, time.Now().UTC()
	a.quotas[input.TenantID] = quota
	return quota, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) CreateQuarantineRecord(ctx context.Context, record contracts.QuarantineRecord) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := quarantineSourceKey(record)
	if _, exists := a.quarantine[key]; exists {
		return false, nil
	}
	a.quarantine[key] = record
	return true, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) ListQuarantineRecordsByStatus(ctx context.Context, status contracts.QuarantineStatus, limit int) (spanResult0 []contracts.QuarantineRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	matched := make([]contracts.QuarantineRecord, 0)
	for _, record := range a.quarantine {
		if record.Status == status {
			matched = append(matched, record)
		}
	}
	return sortAndLimitQuarantine(matched, limit), nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) UpdateQuarantineRelease(ctx context.Context, tenantID string, limit int, resolvedAt time.Time) (spanResult0 []contracts.QuarantineRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	candidates := make([]contracts.QuarantineRecord, 0)
	for _, record := range a.quarantine {
		if isReleasableForTenant(record, tenantID) {
			candidates = append(candidates, record)
		}
	}
	released := sortAndLimitQuarantine(candidates, limit)
	for index := range released {
		released[index].Status, released[index].ResolvedAt = contracts.QuarantineStatusReleased, &resolvedAt
		released[index].Version, released[index].UpdatedAt = released[index].Version+1, resolvedAt
		a.quarantine[quarantineSourceKey(released[index])] = released[index]
	}
	return released, nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) CreateSecurityEvent(ctx context.Context, record contracts.SecurityEventRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, registered := a.tenants[record.ActorTenantID]; !registered {
		return appErrors.NewValidationError("referenced tenant or status does not exist")
	}
	a.securityLog = append(a.securityLog, record)
	return nil
}

func (a *MemoryTenantIsolationRepositoryAdapter) ListSecurityEvents() []contracts.SecurityEventRecord {
	defer observability.TraceEnter(nil)()
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]contracts.SecurityEventRecord(nil), a.securityLog...)
}

func isReleasableForTenant(record contracts.QuarantineRecord, tenantID string) bool {
	return record.Status == contracts.QuarantineStatusPending &&
		record.Reason == contracts.QuarantineReasonUnknownTenant &&
		record.EnvelopeTenantID != nil && *record.EnvelopeTenantID == tenantID
}

func quarantineSourceKey(record contracts.QuarantineRecord) string {
	return fmt.Sprintf("%s|%d|%d", record.SourceTopic, record.SourcePartition, record.SourceOffset)
}

func sortAndLimitQuarantine(records []contracts.QuarantineRecord, limit int) []contracts.QuarantineRecord {
	sort.Slice(records, func(i int, j int) bool {
		if !records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].CreatedAt.Before(records[j].CreatedAt)
		}
		return records[i].ID.String() < records[j].ID.String()
	})
	if limit < len(records) {
		return records[:limit]
	}
	return records
}

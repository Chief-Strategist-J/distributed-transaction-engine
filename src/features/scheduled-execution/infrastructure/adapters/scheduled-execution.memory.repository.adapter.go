package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryScheduledExecutionRepositoryAdapter struct {
	mu          sync.RWMutex
	schedules   map[string]contracts.ScheduleRecord
	occurrences map[string]contracts.ScheduleOccurrenceRecord
}

func NewMemoryScheduledExecutionRepositoryAdapter() *MemoryScheduledExecutionRepositoryAdapter {
	return &MemoryScheduledExecutionRepositoryAdapter{
		schedules:   make(map[string]contracts.ScheduleRecord),
		occurrences: make(map[string]contracts.ScheduleOccurrenceRecord),
	}
}

func (a *MemoryScheduledExecutionRepositoryAdapter) SaveSchedule(ctx context.Context, schedule contracts.ScheduleRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", schedule.TenantID, schedule.ID.String())
	a.schedules[key] = schedule
	return nil
}

func (a *MemoryScheduledExecutionRepositoryAdapter) GetScheduleByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.ScheduleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, id.String())
	sch, exists := a.schedules[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("Schedule not found")
	}
	return &sch, nil
}

func (a *MemoryScheduledExecutionRepositoryAdapter) ListDueSchedules(ctx context.Context, tenantID string, now time.Time, limit int) (spanResult0 []contracts.ScheduleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	var result []contracts.ScheduleRecord
	for _, sch := range a.schedules {
		if sch.TenantID == tenantID && sch.Status == contracts.ScheduleActive && (sch.NextOccurrenceAt.Before(now) || sch.NextOccurrenceAt.Equal(now)) {
			result = append(result, sch)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (a *MemoryScheduledExecutionRepositoryAdapter) AdvanceSchedule(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, nextOccurrence time.Time, lastOccurrence time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, id.String())
	sch, exists := a.schedules[key]
	if !exists {
		return appErrors.NewNotFoundError("Schedule not found")
	}
	if sch.Version != expectedVersion {
		return appErrors.NewConflictError("Schedule version conflict")
	}
	sch.NextOccurrenceAt = nextOccurrence
	sch.LastOccurrenceAt = &lastOccurrence
	sch.Version++
	sch.UpdatedAt = time.Now().UTC()
	a.schedules[key] = sch
	return nil
}

func (a *MemoryScheduledExecutionRepositoryAdapter) SaveOccurrence(ctx context.Context, occ contracts.ScheduleOccurrenceRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%d", occ.TenantID, occ.ScheduleID.String(), occ.LogicalOccurrenceAt.Unix())
	if _, exists := a.occurrences[key]; exists {
		return appErrors.NewConflictError("Occurrence already recorded for logical timestamp")
	}
	a.occurrences[key] = occ
	return nil
}

func (a *MemoryScheduledExecutionRepositoryAdapter) GetOccurrence(ctx context.Context, tenantID string, scheduleID uuid.UUID, logicalTime time.Time) (spanResult0 *contracts.ScheduleOccurrenceRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%d", tenantID, scheduleID.String(), logicalTime.Unix())
	occ, exists := a.occurrences[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("Occurrence not found")
	}
	return &occ, nil
}

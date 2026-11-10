package ports

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"github.com/google/uuid"
)

type ScheduledExecutionRepositoryPort interface {
	SaveSchedule(ctx context.Context, schedule contracts.ScheduleRecord) error
	GetScheduleByID(ctx context.Context, tenantID string, id uuid.UUID) (*contracts.ScheduleRecord, error)
	ListDueSchedules(ctx context.Context, tenantID string, now time.Time, limit int) ([]contracts.ScheduleRecord, error)
	AdvanceSchedule(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, nextOccurrence time.Time, lastOccurrence time.Time) error
	SaveOccurrence(ctx context.Context, occ contracts.ScheduleOccurrenceRecord) error
	GetOccurrence(ctx context.Context, tenantID string, scheduleID uuid.UUID, logicalTime time.Time) (*contracts.ScheduleOccurrenceRecord, error)
}

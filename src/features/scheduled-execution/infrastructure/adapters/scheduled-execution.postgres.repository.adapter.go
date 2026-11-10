package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/features/scheduled-execution/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"time"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type PostgresScheduledExecutionRepositoryAdapter struct {
	db DBExecutor
}

func NewPostgresScheduledExecutionRepositoryAdapter(db DBExecutor) *PostgresScheduledExecutionRepositoryAdapter {
	return &PostgresScheduledExecutionRepositoryAdapter{db: db}
}

func (a *PostgresScheduledExecutionRepositoryAdapter) SaveSchedule(ctx context.Context, schedule contracts.ScheduleRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	args := a.extractScheduleArgs(schedule)
	_, err := a.db.ExecContext(ctx, queries.FLOW_INSERT_SCHEDULE, args...)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresScheduledExecutionRepositoryAdapter) extractScheduleArgs(s contracts.ScheduleRecord) []any {
	return []any{
		s.ID, s.PublicID, s.TenantID, s.Source, s.ExternalID, s.CorrelationID,
		s.WorkflowID, s.ScheduleType, s.CronExpression, s.TimeZone, s.DSTPolicy,
		s.MisfirePolicy, s.OverlapPolicy, s.MaxLatenessSeconds, s.Status,
		s.RunAsPrincipal, s.RequiredPermission, s.FrozenPayload, s.NextOccurrenceAt,
		s.LastOccurrenceAt, s.ConsecutiveFailures, s.Version, s.CreatedAt, s.UpdatedAt,
	}
}

func (a *PostgresScheduledExecutionRepositoryAdapter) GetScheduleByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.ScheduleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, "SELECT id, public_id, tenant_id, source, external_id, correlation_id, workflow_id, schedule_type, cron_expression, time_zone, dst_policy, misfire_policy, overlap_policy, max_lateness_seconds, status, run_as_principal, required_permission, frozen_payload, next_occurrence_at, last_occurrence_at, consecutive_failures, version, created_at, updated_at FROM schedules WHERE tenant_id = $1 AND id = $2;", tenantID, id)
	return a.scanSchedule(row)
}

func (a *PostgresScheduledExecutionRepositoryAdapter) ListDueSchedules(ctx context.Context, tenantID string, now time.Time, limit int) (spanResult0 []contracts.ScheduleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_SELECT_DUE_SCHEDULES, tenantID, now, limit)
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}
	defer rows.Close()

	var result []contracts.ScheduleRecord
	for rows.Next() {
		sch, scanErr := a.scanScheduleRows(rows)
		if scanErr != nil {
			return nil, appErrors.NewInternalError(scanErr.Error())
		}
		result = append(result, *sch)
	}
	return result, nil
}

func (a *PostgresScheduledExecutionRepositoryAdapter) AdvanceSchedule(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, nextOccurrence time.Time, lastOccurrence time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_ADVANCE_SCHEDULE, nextOccurrence, lastOccurrence, tenantID, id, expectedVersion)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil || rowsAffected == 0 {
		return appErrors.NewConflictError("Schedule version conflict or record missing")
	}
	return nil
}

func (a *PostgresScheduledExecutionRepositoryAdapter) SaveOccurrence(ctx context.Context, occ contracts.ScheduleOccurrenceRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_INSERT_OCCURRENCE,
		occ.ID, occ.PublicID, occ.TenantID, occ.Source, occ.ExternalID,
		occ.CorrelationID, occ.ScheduleID, occ.LogicalOccurrenceAt,
		occ.Status, occ.DispatchedSagaID, occ.FailureReason, occ.Version, occ.CreatedAt,
	)
	if err != nil {
		return appErrors.NewConflictError("Failed to record occurrence; may already exist: " + err.Error())
	}
	return nil
}

func (a *PostgresScheduledExecutionRepositoryAdapter) GetOccurrence(ctx context.Context, tenantID string, scheduleID uuid.UUID, logicalTime time.Time) (spanResult0 *contracts.ScheduleOccurrenceRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, "SELECT id, public_id, tenant_id, source, external_id, correlation_id, schedule_id, logical_occurrence_at, status, dispatched_saga_id, failure_reason, version, created_at FROM schedule_occurrences WHERE tenant_id = $1 AND schedule_id = $2 AND logical_occurrence_at = $3;", tenantID, scheduleID, logicalTime)
	var rec contracts.ScheduleOccurrenceRecord
	err := row.Scan(&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID, &rec.CorrelationID, &rec.ScheduleID, &rec.LogicalOccurrenceAt, &rec.Status, &rec.DispatchedSagaID, &rec.FailureReason, &rec.Version, &rec.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, appErrors.NewNotFoundError("Occurrence not found")
		}
		return nil, appErrors.NewInternalError(err.Error())
	}
	return &rec, nil
}

func (a *PostgresScheduledExecutionRepositoryAdapter) scanSchedule(row *sql.Row) (*contracts.ScheduleRecord, error) {
	var sch contracts.ScheduleRecord
	err := row.Scan(
		&sch.ID, &sch.PublicID, &sch.TenantID, &sch.Source, &sch.ExternalID,
		&sch.CorrelationID, &sch.WorkflowID, &sch.ScheduleType, &sch.CronExpression,
		&sch.TimeZone, &sch.DSTPolicy, &sch.MisfirePolicy, &sch.OverlapPolicy,
		&sch.MaxLatenessSeconds, &sch.Status, &sch.RunAsPrincipal, &sch.RequiredPermission,
		&sch.FrozenPayload, &sch.NextOccurrenceAt, &sch.LastOccurrenceAt, &sch.ConsecutiveFailures,
		&sch.Version, &sch.CreatedAt, &sch.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, appErrors.NewNotFoundError("Schedule not found")
		}
		return nil, appErrors.NewInternalError(err.Error())
	}
	return &sch, nil
}

func (a *PostgresScheduledExecutionRepositoryAdapter) scanScheduleRows(rows *sql.Rows) (*contracts.ScheduleRecord, error) {
	var sch contracts.ScheduleRecord
	err := rows.Scan(
		&sch.ID, &sch.PublicID, &sch.TenantID, &sch.Source, &sch.ExternalID,
		&sch.CorrelationID, &sch.WorkflowID, &sch.ScheduleType, &sch.CronExpression,
		&sch.TimeZone, &sch.DSTPolicy, &sch.MisfirePolicy, &sch.OverlapPolicy,
		&sch.MaxLatenessSeconds, &sch.Status, &sch.RunAsPrincipal, &sch.RequiredPermission,
		&sch.FrozenPayload, &sch.NextOccurrenceAt, &sch.LastOccurrenceAt, &sch.ConsecutiveFailures,
		&sch.Version, &sch.CreatedAt, &sch.UpdatedAt,
	)
	return &sch, err
}

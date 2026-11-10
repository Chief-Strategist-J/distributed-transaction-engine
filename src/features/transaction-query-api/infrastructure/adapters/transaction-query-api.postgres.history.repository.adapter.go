package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"strconv"
	"time"
)

type PostgresTransactionHistoryRepositoryAdapter struct {
	view *PostgresTransactionViewRepositoryAdapter
}

func NewPostgresTransactionHistoryRepositoryAdapter(db DBTransactor) *PostgresTransactionHistoryRepositoryAdapter {
	return &PostgresTransactionHistoryRepositoryAdapter{view: NewPostgresTransactionViewRepositoryAdapter(db)}
}

func (a *PostgresTransactionHistoryRepositoryAdapter) ListTimelineEvents(ctx context.Context, tenantID string, sagaID uuid.UUID, after *contracts.TimelinePosition, limit int) (spanResult0 []contracts.TimelineRow, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	statement, args := compileTimelineQuery(tenantID, sagaID, after, limit)
	var result []contracts.TimelineRow
	err := a.view.runReadOnly(ctx, tenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, statement, args...)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		var scanErr error
		result, scanErr = scanTimelineRows(rows)
		return scanErr
	})
	return result, err
}

func compileTimelineQuery(tenantID string, sagaID uuid.UUID, after *contracts.TimelinePosition, limit int) (string, []any) {
	args := []any{tenantID, sagaID}
	statement := queries.FLOW_SELECT_TIMELINE_EVENTS_PREFIX
	if after != nil {
		statement += " AND (created_at, step_id) > ($3, $4)"
		args = append(args, after.CreatedAt, after.StepID)
	}
	args = append(args, limit)
	return statement + " ORDER BY created_at, step_id LIMIT $" + strconv.Itoa(len(args)), args
}

func scanTimelineRows(rows *sql.Rows) ([]contracts.TimelineRow, error) {
	defer rows.Close()
	result := make([]contracts.TimelineRow, 0)
	for rows.Next() {
		var row contracts.TimelineRow
		var started, completed sql.NullTime
		var created time.Time
		if err := rows.Scan(&row.StepID, &row.Event.State, &row.Event.Attempts, &row.RawMessage, &row.Event.IdempotencyKey, &started, &completed, &created); err != nil {
			return nil, mapViewError(err)
		}
		row.Event.StepName, row.CreatedAt = row.StepID, created.UTC()
		if started.Valid {
			instant := started.Time.UTC()
			row.Event.StartedAt = &instant
		}
		if completed.Valid {
			instant := completed.Time.UTC()
			row.Event.CompletedAt = &instant
		}
		result = append(result, row)
	}
	return result, mapViewError(rows.Err())
}

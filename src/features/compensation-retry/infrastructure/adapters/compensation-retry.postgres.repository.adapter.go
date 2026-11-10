package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/ports"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type PostgresCompensationRetryRepositoryAdapter struct {
	db DBExecutor
}

var _ ports.RepositoryPort = (*PostgresCompensationRetryRepositoryAdapter)(nil)

func NewPostgresCompensationRetryRepositoryAdapter(db DBExecutor) *PostgresCompensationRetryRepositoryAdapter {
	return &PostgresCompensationRetryRepositoryAdapter{db: db}
}

func mapDBConstraintError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "compensation_retries_step_uk") {
		return appErrors.NewConflictError("duplicate step compensation attempt within saga")
	}
	if strings.Contains(msg, "compensation_retries_public_id_uk") {
		return appErrors.NewConflictError("duplicate public_id in compensation retries")
	}
	if strings.Contains(msg, "compensation_retries_status_ck") {
		return appErrors.NewValidationError("invalid compensation retry status")
	}
	if strings.Contains(msg, "compensation_retries_public_id_ck") {
		return appErrors.NewValidationError("malformed public_id format")
	}
	if strings.Contains(msg, "compensation_retries_max_attempts_ck") {
		return appErrors.NewValidationError("max attempts must be between 1 and 100")
	}
	return appErrors.NewInternalError("database operation failed: " + msg)
}

func (a *PostgresCompensationRetryRepositoryAdapter) UpsertAttempt(ctx context.Context, record contracts.CompensationRetryRecord) (spanResult0 *contracts.CompensationRetryRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var r contracts.CompensationRetryRecord
	err := a.db.QueryRowContext(
		ctx,
		queries.QueryUpsertCompensationRetry,
		record.ID, record.PublicID, record.TenantID, record.Source,
		record.ExternalID, record.CorrelationID, record.SagaID,
		record.StepID, record.StepType, record.DependencyID,
		record.AttemptNumber, record.MaxAttempts, record.FirstAttemptAt,
		record.BusinessDeadline, record.NextRetryAt, record.Status,
		record.LastError,
	).Scan(
		&r.ID, &r.PublicID, &r.TenantID, &r.Source,
		&r.ExternalID, &r.CorrelationID, &r.SagaID,
		&r.StepID, &r.StepType, &r.DependencyID,
		&r.AttemptNumber, &r.MaxAttempts, &r.FirstAttemptAt,
		&r.BusinessDeadline, &r.NextRetryAt, &r.Status,
		&r.LastError, &r.Version, &r.CreatedAt, &r.UpdatedAt,
	)

	if err != nil {
		return nil, mapDBConstraintError(err)
	}

	return &r, nil
}

func (a *PostgresCompensationRetryRepositoryAdapter) GetRecord(ctx context.Context, tenantID string, sagaID uuid.UUID, stepID string) (spanResult0 *contracts.CompensationRetryRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var r contracts.CompensationRetryRecord
	err := a.db.QueryRowContext(
		ctx,
		queries.QueryGetCompensationRetry,
		tenantID, sagaID, stepID,
	).Scan(
		&r.ID, &r.PublicID, &r.TenantID, &r.Source,
		&r.ExternalID, &r.CorrelationID, &r.SagaID,
		&r.StepID, &r.StepType, &r.DependencyID,
		&r.AttemptNumber, &r.MaxAttempts, &r.FirstAttemptAt,
		&r.BusinessDeadline, &r.NextRetryAt, &r.Status,
		&r.LastError, &r.Version, &r.CreatedAt, &r.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, appErrors.NewNotFoundError("compensation retry record not found")
	}
	if err != nil {
		return nil, mapDBConstraintError(err)
	}

	return &r, nil
}

func (a *PostgresCompensationRetryRepositoryAdapter) UpdateStatus(ctx context.Context, tenantID string, sagaID uuid.UUID, stepID string, status string, nextRetryAt *time.Time, lastError string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(
		ctx,
		queries.QueryUpdateCompensationStatus,
		status, nextRetryAt, lastError, tenantID, sagaID, stepID,
	)
	if err != nil {
		return mapDBConstraintError(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return appErrors.NewInternalError("failed to check affected rows: " + err.Error())
	}
	if rows == 0 {
		return appErrors.NewNotFoundError("compensation retry record not found for update")
	}

	return nil
}

func (a *PostgresCompensationRetryRepositoryAdapter) PauseStepType(ctx context.Context, tenantID string, stepType string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	pubID := fmt.Sprintf("stp_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26])
	extID := fmt.Sprintf("%s:%s", tenantID, stepType)
	_, err := a.db.ExecContext(
		ctx,
		queries.QueryUpsertStepTypePause,
		pubID, tenantID, extID, stepType, true, "administrative pause",
	)
	if err != nil {
		return mapDBConstraintError(err)
	}
	return nil
}

func (a *PostgresCompensationRetryRepositoryAdapter) ResumeStepType(ctx context.Context, tenantID string, stepType string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	pubID := fmt.Sprintf("stp_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26])
	extID := fmt.Sprintf("%s:%s", tenantID, stepType)
	_, err := a.db.ExecContext(
		ctx,
		queries.QueryUpsertStepTypePause,
		pubID, tenantID, extID, stepType, false, "resumed",
	)
	if err != nil {
		return mapDBConstraintError(err)
	}
	return nil
}

func (a *PostgresCompensationRetryRepositoryAdapter) IsStepTypePaused(ctx context.Context, tenantID string, stepType string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var isPaused bool
	err := a.db.QueryRowContext(
		ctx,
		queries.QueryGetStepTypePause,
		tenantID, stepType,
	).Scan(&isPaused)

	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, mapDBConstraintError(err)
	}
	return isPaused, nil
}

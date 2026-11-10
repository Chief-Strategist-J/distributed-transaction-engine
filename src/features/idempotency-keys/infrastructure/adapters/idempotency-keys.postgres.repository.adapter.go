package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/features/idempotency-keys/domain/rules"
	"distributed-transaction-engine/src/features/idempotency-keys/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"time"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type PostgresIdempotencyRepositoryAdapter struct {
	db DBExecutor
}

func NewPostgresIdempotencyRepositoryAdapter(db DBExecutor) *PostgresIdempotencyRepositoryAdapter {
	return &PostgresIdempotencyRepositoryAdapter{db: db}
}

func (a *PostgresIdempotencyRepositoryAdapter) Acquire(
	ctx context.Context,
	record contracts.IdempotencyRecord,
) (spanResult0 *contracts.AcquireResult, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	now := time.Now().UTC()
	var insertedID string

	err := a.db.QueryRowContext(
		ctx,
		queries.FLOW_INSERT_IDEMPOTENCY_KEY,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.Source,
		record.ExternalID,
		record.CorrelationID,
		record.PrincipalID,
		record.OperationID,
		record.IdempotencyKey,
		record.RequestFingerprint,
		record.CurrentState,
		record.LeaseOwner,
		record.LeaseUntil,
		record.RecoveryFencingToken,
		now,
		now,
		record.ExpiresAt,
	).Scan(&insertedID)

	if err == nil {
		recCopy := record
		recCopy.CreatedAt = now
		recCopy.UpdatedAt = now
		return &contracts.AcquireResult{
			Outcome: contracts.OutcomeAcquired,
			Record:  &recCopy,
		}, nil
	}

	if err != sql.ErrNoRows {
		return nil, appErrors.NewInternalError(err.Error())
	}

	existing, err := a.GetByScope(ctx, record.TenantID, record.PrincipalID, record.OperationID, record.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, appErrors.NewInternalError("concurrent insert detected but record not found")
	}

	outcome := rules.EvaluateExistingRecord(existing, record.RequestFingerprint, now)
	var snapshot *contracts.ResponseSnapshot
	if existing.ResponseStatus != nil {
		snapshot = &contracts.ResponseSnapshot{
			StatusCode: *existing.ResponseStatus,
			Headers:    existing.ResponseHeaders,
			Body:       existing.ResponseBody,
		}
	}

	return &contracts.AcquireResult{
		Outcome:  outcome,
		Record:   existing,
		Snapshot: snapshot,
	}, nil
}

func (a *PostgresIdempotencyRepositoryAdapter) GetByScope(
	ctx context.Context,
	tenantID, principalID, operationID, idempotencyKey string,
) (spanResult0 *contracts.IdempotencyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_BY_SCOPE, tenantID, principalID, operationID, idempotencyKey)

	var rec contracts.IdempotencyRecord
	var stateStr string
	var respStatus sql.NullInt64
	var headersBytes, bodyBytes []byte
	var leaseUntil, completedAt sql.NullTime

	err := row.Scan(
		&rec.ID,
		&rec.PublicID,
		&rec.TenantID,
		&rec.Source,
		&rec.ExternalID,
		&rec.CorrelationID,
		&rec.PrincipalID,
		&rec.OperationID,
		&rec.IdempotencyKey,
		&rec.RequestFingerprint,
		&stateStr,
		&respStatus,
		&headersBytes,
		&bodyBytes,
		&rec.ResourceReference,
		&rec.LeaseOwner,
		&leaseUntil,
		&rec.RecoveryFencingToken,
		&rec.CreatedAt,
		&rec.UpdatedAt,
		&completedAt,
		&rec.ExpiresAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, appErrors.NewInternalError(err.Error())
	}

	rec.CurrentState = contracts.IdempotencyState(stateStr)
	if respStatus.Valid {
		val := int(respStatus.Int64)
		rec.ResponseStatus = &val
	}
	if len(headersBytes) > 0 {
		observability.NoteSwallowed(json.Unmarshal(headersBytes, &rec.ResponseHeaders))
	}
	if len(bodyBytes) > 0 {
		observability.NoteSwallowed(json.Unmarshal(bodyBytes, &rec.ResponseBody))
	}
	if leaseUntil.Valid {
		t := leaseUntil.Time.UTC()
		rec.LeaseUntil = &t
	}
	if completedAt.Valid {
		t := completedAt.Time.UTC()
		rec.CompletedAt = &t
	}

	return &rec, nil
}

func (a *PostgresIdempotencyRepositoryAdapter) CommitOutcome(
	ctx context.Context,
	tenantID, id string,
	state contracts.IdempotencyState,
	snapshot contracts.ResponseSnapshot,
	resourceRef string,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	headersBytes, err := json.Marshal(snapshot.Headers)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	bodyBytes, err := json.Marshal(snapshot.Body)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}

	now := time.Now().UTC()
	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_COMMIT_OUTCOME,
		string(state),
		snapshot.StatusCode,
		headersBytes,
		bodyBytes,
		resourceRef,
		now,
		now,
		tenantID,
		id,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	if rows == 0 {
		return appErrors.NewConflictError("idempotency record not found for commit")
	}

	return nil
}

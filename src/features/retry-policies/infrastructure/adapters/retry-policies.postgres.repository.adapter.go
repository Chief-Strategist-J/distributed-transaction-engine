package adapters

import (
	"context"
	"database/sql"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
	"distributed-transaction-engine/src/features/retry-policies/infrastructure/queries"
	appErrors "distributed-transaction-engine/src/shared/errors"

	"github.com/google/uuid"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type PostgresRetryRepositoryAdapter struct {
	db DBExecutor
}

func NewPostgresRetryRepositoryAdapter(db DBExecutor) *PostgresRetryRepositoryAdapter {
	return &PostgresRetryRepositoryAdapter{db: db}
}

func (a *PostgresRetryRepositoryAdapter) RecordStepAttempt(
	ctx context.Context,
	tenantID, sagaID, stepID string,
	attempt int,
	errClass, errMsg, errFp string,
	retryAfter int,
	delayMillis int64,
	nextAttempt *time.Time,
) error {
	id := uuid.New()
	publicID := "sre_" + id.String()[:26]
	now := time.Now().UTC()

	_, err := a.db.ExecContext(
		ctx,
		queries.FLOW_INSERT_STEP_RETRY,
		id,
		publicID,
		tenantID,
		sagaID,
		stepID,
		attempt,
		errClass,
		errMsg,
		errFp,
		retryAfter,
		delayMillis,
		nextAttempt,
		now,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresRetryRepositoryAdapter) GetPolicy(
	ctx context.Context,
	tenantID, policyName string,
) (*contracts.RetryPolicyConfig, error) {
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_RETRY_POLICY, tenantID, policyName)

	var p contracts.RetryPolicyConfig
	var algo string
	err := row.Scan(
		&p.ID,
		&p.PublicID,
		&p.TenantID,
		&p.PolicyName,
		&p.MaxAttempts,
		&p.MaxElapsedSeconds,
		&p.BaseIntervalMillis,
		&p.MaxIntervalMillis,
		&algo,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, appErrors.NewInternalError(err.Error())
	}

	p.Algorithm = contracts.BackoffAlgorithm(algo)
	return &p, nil
}

func (a *PostgresRetryRepositoryAdapter) SavePolicy(
	ctx context.Context,
	policy contracts.RetryPolicyConfig,
) error {
	now := time.Now().UTC()
	extID := "ext_rtp_" + policy.PolicyName
	corrID := uuid.New()

	_, err := a.db.ExecContext(
		ctx,
		queries.FLOW_UPSERT_RETRY_POLICY,
		policy.ID,
		policy.PublicID,
		policy.TenantID,
		"ENGINE_CONFIG",
		extID,
		corrID,
		policy.PolicyName,
		policy.MaxAttempts,
		policy.MaxElapsedSeconds,
		policy.BaseIntervalMillis,
		policy.MaxIntervalMillis,
		string(policy.Algorithm),
		now,
		now,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

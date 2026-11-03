package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"distributed-transaction-engine/src/features/crash-recovery/contracts"
	"distributed-transaction-engine/src/features/crash-recovery/infrastructure/queries"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type PostgresCrashRecoveryRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresCrashRecoveryRepositoryAdapter(db *sql.DB) *PostgresCrashRecoveryRepositoryAdapter {
	return &PostgresCrashRecoveryRepositoryAdapter{db: db}
}

func (a *PostgresCrashRecoveryRepositoryAdapter) FindExpiredLeases(ctx context.Context, batchSize int) ([]contracts.OrphanCandidate, error) {
	now := time.Now().UTC()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_FIND_EXPIRED_LEASES, now, batchSize)
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	defer rows.Close()

	var results []contracts.OrphanCandidate
	for rows.Next() {
		var c contracts.OrphanCandidate
		var stateStr string
		var lUntil sql.NullTime
		var initPayloadBytes []byte

		err := rows.Scan(
			&c.ID,
			&c.PublicID,
			&c.TenantID,
			&stateStr,
			&c.FencingToken,
			&c.WorkflowDefinitionID,
			&c.WorkflowVersion,
			&c.SpecHash,
			&c.LeaseOwner,
			&lUntil,
			&c.TakeoverCount,
			&initPayloadBytes,
		)
		if err != nil {
			return nil, errors.NewInternalError(err.Error())
		}

		c.CurrentState = sagaContracts.SagaState(stateStr)
		if lUntil.Valid {
			t := lUntil.Time.UTC()
			c.LeaseUntil = &t
		}
		if len(initPayloadBytes) > 0 {
			var m map[string]interface{}
			if err := json.Unmarshal(initPayloadBytes, &m); err == nil {
				c.InitialPayload = m
			}
		}

		results = append(results, c)
	}

	return results, nil
}

func (a *PostgresCrashRecoveryRepositoryAdapter) TakeoverLease(
	ctx context.Context,
	tenantID string,
	sagaID string,
	observedToken int64,
	incarnationID string,
	leaseDuration time.Duration,
) (*contracts.TakeoverResult, error) {
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)

	row := a.db.QueryRowContext(
		ctx,
		queries.FLOW_TAKEOVER_LEASE,
		incarnationID,
		leaseUntil,
		now,
		tenantID,
		sagaID,
		observedToken,
	)

	var newToken int64
	var takeoverCount int
	err := row.Scan(&newToken, &takeoverCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.NewInternalError(err.Error())
	}

	return &contracts.TakeoverResult{
		SagaID:        sagaID,
		TenantID:      tenantID,
		NewToken:      newToken,
		TakeoverCount: takeoverCount,
		IsPoisoned:    false,
	}, nil
}

func (a *PostgresCrashRecoveryRepositoryAdapter) MarkPoisoned(ctx context.Context, tenantID string, sagaID string, token int64) error {
	now := time.Now().UTC()
	_, err := a.db.ExecContext(ctx, queries.FLOW_MARK_POISONED, now, tenantID, sagaID, token)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresCrashRecoveryRepositoryAdapter) RenewHeartbeat(
	ctx context.Context,
	tenantID string,
	sagaID string,
	incarnationID string,
	token int64,
	leaseDuration time.Duration,
) (bool, error) {
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)

	res, err := a.db.ExecContext(ctx, queries.FLOW_RENEW_HEARTBEAT, leaseUntil, now, tenantID, sagaID, incarnationID, token)
	if err != nil {
		return false, errors.NewInternalError(err.Error())
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, errors.NewInternalError(err.Error())
	}

	return affected > 0, nil
}

func (a *PostgresCrashRecoveryRepositoryAdapter) LoadStepExecutions(ctx context.Context, tenantID string, sagaID string) ([]sagaContracts.SagaStepExecution, error) {
	rows, err := a.db.QueryContext(ctx, queries.FLOW_LOAD_STEP_EXECUTIONS, tenantID, sagaID)
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	defer rows.Close()

	var steps []sagaContracts.SagaStepExecution
	for rows.Next() {
		var s sagaContracts.SagaStepExecution
		var stateStr string
		var inputBytes, outputBytes []byte

		err := rows.Scan(
			&s.ID,
			&s.PublicID,
			&s.TenantID,
			&s.SagaID,
			&s.StepID,
			&stateStr,
			&s.AttemptCount,
			&inputBytes,
			&outputBytes,
			&s.ErrorMessage,
			&s.IdempotencyKey,
		)
		if err != nil {
			return nil, errors.NewInternalError(err.Error())
		}

		s.ExecutionState = sagaContracts.StepExecutionState(stateStr)
		if len(inputBytes) > 0 {
			_ = json.Unmarshal(inputBytes, &s.StepInput)
		}
		if len(outputBytes) > 0 {
			_ = json.Unmarshal(outputBytes, &s.StepOutput)
		}
		steps = append(steps, s)
	}

	return steps, nil
}

func (a *PostgresCrashRecoveryRepositoryAdapter) UpdateStepExecution(ctx context.Context, tenantID string, sagaID string, step *sagaContracts.SagaStepExecution) error {
	now := time.Now().UTC()
	outBytes, err := json.Marshal(step.StepOutput)
	if err != nil {
		outBytes = []byte("{}")
	}

	_, err = a.db.ExecContext(
		ctx,
		queries.FLOW_UPDATE_STEP_EXECUTION,
		string(step.ExecutionState),
		step.AttemptCount,
		outBytes,
		step.ErrorMessage,
		now,
		tenantID,
		sagaID,
		step.StepID,
	)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}

	return nil
}

func (a *PostgresCrashRecoveryRepositoryAdapter) ReleaseLease(ctx context.Context, tenantID string, sagaID string, token int64) error {
	now := time.Now().UTC()
	_, err := a.db.ExecContext(ctx, queries.FLOW_RELEASE_LEASE, now, tenantID, sagaID, token)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	return nil
}

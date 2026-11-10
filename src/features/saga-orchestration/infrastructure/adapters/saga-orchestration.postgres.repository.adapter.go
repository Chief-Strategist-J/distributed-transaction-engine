package adapters

import (
	"context"
	"database/sql"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	ports "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	"distributed-transaction-engine/src/features/saga-orchestration/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"strings"
	"time"
)

type PostgresSagaRepositoryAdapter struct {
	db *sql.DB
}

var _ ports.SagaRepositoryPort = (*PostgresSagaRepositoryAdapter)(nil)

func NewPostgresSagaRepositoryAdapter(db *sql.DB) *PostgresSagaRepositoryAdapter {
	return &PostgresSagaRepositoryAdapter{db: db}
}

func (a *PostgresSagaRepositoryAdapter) CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	defer tx.Rollback()

	initPayloadJSON, err := json.Marshal(instance.InitialPayload)
	if err != nil {
		return errors.NewValidationError("failed to serialize initialPayload")
	}
	finalOutputJSON, err := json.Marshal(instance.FinalOutput)
	if err != nil {
		return errors.NewValidationError("failed to serialize finalOutput")
	}

	var leaseUntil sql.NullTime
	if instance.LeaseUntil != "" {
		if t, parseErr := time.Parse(time.RFC3339Nano, instance.LeaseUntil); parseErr == nil {
			leaseUntil = sql.NullTime{Time: t, Valid: true}
		}
	}

	deadlineAt, err := time.Parse(time.RFC3339Nano, instance.DeadlineAt)
	if err != nil {
		deadlineAt = time.Now().UTC().Add(30 * time.Minute)
	}

	_, err = tx.ExecContext(
		ctx,
		queries.InsertSagaInstance,
		instance.ID,
		instance.PublicID,
		instance.TenantID,
		instance.WorkflowDefinitionID,
		instance.WorkflowVersion,
		instance.SpecHash,
		instance.Source,
		instance.ExternalID,
		instance.InputHash,
		instance.CorrelationID,
		string(instance.CurrentState),
		instance.Version,
		instance.FencingToken,
		instance.LeaseOwner,
		leaseUntil,
		deadlineAt,
		initPayloadJSON,
		finalOutputJSON,
		instance.CreatedAt,
		instance.UpdatedAt,
	)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return errors.NewConflictError("saga instance already exists")
		}
		return errors.NewInternalError(err.Error())
	}

	for _, step := range instance.Steps {
		stepInputJSON, _ := json.Marshal(step.StepInput)
		stepOutputJSON, _ := json.Marshal(step.StepOutput)

		var startedAt sql.NullTime
		if step.StartedAt != "" {
			if t, parseErr := time.Parse(time.RFC3339Nano, step.StartedAt); parseErr == nil {
				startedAt = sql.NullTime{Time: t, Valid: true}
			}
		}

		var completedAt sql.NullTime
		if step.CompletedAt != "" {
			if t, parseErr := time.Parse(time.RFC3339Nano, step.CompletedAt); parseErr == nil {
				completedAt = sql.NullTime{Time: t, Valid: true}
			}
		}

		_, err = tx.ExecContext(
			ctx,
			queries.InsertSagaStepExecution,
			step.ID,
			step.PublicID,
			step.TenantID,
			step.SagaID,
			step.StepID,
			string(step.ExecutionState),
			step.AttemptCount,
			stepInputJSON,
			stepOutputJSON,
			step.ErrorMessage,
			step.IdempotencyKey,
			startedAt,
			completedAt,
			step.CreatedAt,
			step.UpdatedAt,
		)
		if err != nil {
			return errors.NewInternalError(err.Error())
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresSagaRepositoryAdapter) GetSagaByID(ctx context.Context, tenantID string, id string) (spanResult0 *sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.SelectSagaByID, tenantID, id)
	return a.scanSagaWithSteps(ctx, row, tenantID)
}

func (a *PostgresSagaRepositoryAdapter) GetSagaByPublicID(ctx context.Context, tenantID string, publicID string) (spanResult0 *sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.SelectSagaByPublicID, tenantID, publicID)
	return a.scanSagaWithSteps(ctx, row, tenantID)
}

func (a *PostgresSagaRepositoryAdapter) GetSagaByExternalID(ctx context.Context, tenantID string, workflowDefinitionID string, externalID string) (spanResult0 *sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.SelectSagaByExternalID, tenantID, workflowDefinitionID, externalID)
	return a.scanSagaWithSteps(ctx, row, tenantID)
}

func (a *PostgresSagaRepositoryAdapter) UpdateSagaState(ctx context.Context, tenantID string, id string, expectedVersion int64, nextState sagaContracts.SagaState) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	now := time.Now().UTC()
	res, err := a.db.ExecContext(ctx, queries.UpdateSagaStateCAS, string(nextState), now, tenantID, id, expectedVersion)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	if rowsAffected == 0 {
		return errors.NewConflictError("concurrency conflict: optimistic lock failed on saga state transition")
	}
	return nil
}

func (a *PostgresSagaRepositoryAdapter) UpdateSagaStateWithFencing(ctx context.Context, tenantID string, id string, expectedVersion int64, fencingToken int64, nextState sagaContracts.SagaState) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	now := time.Now().UTC()
	res, err := a.db.ExecContext(ctx, queries.UpdateSagaStateWithFencing, string(nextState), now, tenantID, id, expectedVersion, fencingToken)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	if rowsAffected == 0 {
		return errors.NewConflictError("concurrency conflict: optimistic lock or fencing token mismatch")
	}
	return nil
}

func (a *PostgresSagaRepositoryAdapter) AcquireNextLease(ctx context.Context, workerID string, leaseDuration time.Duration) (spanResult0 *sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)

	row := a.db.QueryRowContext(ctx, queries.AcquireNextSagaLease, workerID, leaseUntil, now)
	var s sagaContracts.SagaInstance
	var stateStr string
	var initJSON, finalJSON []byte
	var created, updated, deadline time.Time
	var lUntil sql.NullTime

	err := row.Scan(
		&s.ID,
		&s.PublicID,
		&s.TenantID,
		&s.WorkflowDefinitionID,
		&s.WorkflowVersion,
		&s.SpecHash,
		&s.Source,
		&s.ExternalID,
		&s.InputHash,
		&s.CorrelationID,
		&stateStr,
		&s.Version,
		&s.FencingToken,
		&s.LeaseOwner,
		&lUntil,
		&deadline,
		&initJSON,
		&finalJSON,
		&created,
		&updated,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.NewInternalError(err.Error())
	}

	s.CurrentState = sagaContracts.SagaState(stateStr)
	s.CreatedAt = created.Format(time.RFC3339Nano)
	s.UpdatedAt = updated.Format(time.RFC3339Nano)
	s.DeadlineAt = deadline.Format(time.RFC3339Nano)
	if lUntil.Valid {
		s.LeaseUntil = lUntil.Time.Format(time.RFC3339Nano)
	}
	observability.NoteSwallowed(json.Unmarshal(initJSON, &s.InitialPayload))
	observability.NoteSwallowed(json.Unmarshal(finalJSON, &s.FinalOutput))

	stepRows, err := a.db.QueryContext(ctx, queries.SelectSagaSteps, s.TenantID, s.ID)
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	defer stepRows.Close()

	for stepRows.Next() {
		var step sagaContracts.SagaStepExecution
		var stepStateStr string
		var stepInJSON, stepOutJSON []byte
		var started, completed sql.NullTime
		var stepCreated, stepUpdated time.Time

		err := stepRows.Scan(
			&step.ID,
			&step.PublicID,
			&step.TenantID,
			&step.SagaID,
			&step.StepID,
			&stepStateStr,
			&step.AttemptCount,
			&stepInJSON,
			&stepOutJSON,
			&step.ErrorMessage,
			&step.IdempotencyKey,
			&started,
			&completed,
			&stepCreated,
			&stepUpdated,
		)
		if err != nil {
			return nil, errors.NewInternalError(err.Error())
		}

		step.ExecutionState = sagaContracts.StepExecutionState(stepStateStr)
		step.CreatedAt = stepCreated.Format(time.RFC3339Nano)
		step.UpdatedAt = stepUpdated.Format(time.RFC3339Nano)
		if started.Valid {
			step.StartedAt = started.Time.Format(time.RFC3339Nano)
		}
		if completed.Valid {
			step.CompletedAt = completed.Time.Format(time.RFC3339Nano)
		}
		observability.NoteSwallowed(json.Unmarshal(stepInJSON, &step.StepInput))
		observability.NoteSwallowed(json.Unmarshal(stepOutJSON, &step.StepOutput))

		s.Steps = append(s.Steps, step)
	}

	return &s, nil
}

func (a *PostgresSagaRepositoryAdapter) ReleaseLease(ctx context.Context, tenantID string, id string, fencingToken int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	now := time.Now().UTC()
	res, err := a.db.ExecContext(ctx, queries.ReleaseSagaLease, now, tenantID, id, fencingToken)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	if rowsAffected == 0 {
		return errors.NewConflictError("fencing conflict: stale worker cannot release lease")
	}
	return nil
}

func (a *PostgresSagaRepositoryAdapter) AcquireLeaseForSaga(ctx context.Context, tenantID string, id string, workerID string, leaseDuration time.Duration) (spanResult0 *sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)

	row := a.db.QueryRowContext(ctx, queries.AcquireSagaLeaseByID, workerID, leaseUntil, now, tenantID, id)
	return a.scanSagaWithSteps(ctx, row, tenantID)
}

func (a *PostgresSagaRepositoryAdapter) UpdateStepExecution(ctx context.Context, tenantID string, sagaID string, step *sagaContracts.SagaStepExecution) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	stepOutputJSON, _ := json.Marshal(step.StepOutput)
	now := time.Now().UTC()

	var completedAt sql.NullTime
	if step.CompletedAt != "" {
		if t, parseErr := time.Parse(time.RFC3339Nano, step.CompletedAt); parseErr == nil {
			completedAt = sql.NullTime{Time: t, Valid: true}
		}
	}

	res, err := a.db.ExecContext(
		ctx,
		queries.UpdateStepExecution,
		string(step.ExecutionState),
		step.AttemptCount,
		stepOutputJSON,
		step.ErrorMessage,
		completedAt,
		now,
		tenantID,
		sagaID,
		step.StepID,
	)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	if rowsAffected == 0 {
		return errors.NewNotFoundError("step execution not found: " + step.StepID)
	}
	return nil
}

func (a *PostgresSagaRepositoryAdapter) ListSagas(ctx context.Context, tenantID string, limit int, offset int) (spanResult0 []sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.ListSagasByTenant, tenantID, limit, offset)
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	defer rows.Close()

	var sagas []sagaContracts.SagaInstance
	for rows.Next() {
		var s sagaContracts.SagaInstance
		var stateStr string
		var initJSON, finalJSON []byte
		var created, updated, deadline time.Time
		var leaseUntil sql.NullTime

		err := rows.Scan(
			&s.ID,
			&s.PublicID,
			&s.TenantID,
			&s.WorkflowDefinitionID,
			&s.WorkflowVersion,
			&s.SpecHash,
			&s.Source,
			&s.ExternalID,
			&s.InputHash,
			&s.CorrelationID,
			&stateStr,
			&s.Version,
			&s.FencingToken,
			&s.LeaseOwner,
			&leaseUntil,
			&deadline,
			&initJSON,
			&finalJSON,
			&created,
			&updated,
		)
		if err != nil {
			return nil, errors.NewInternalError(err.Error())
		}

		s.CurrentState = sagaContracts.SagaState(stateStr)
		s.CreatedAt = created.Format(time.RFC3339Nano)
		s.UpdatedAt = updated.Format(time.RFC3339Nano)
		s.DeadlineAt = deadline.Format(time.RFC3339Nano)
		if leaseUntil.Valid {
			s.LeaseUntil = leaseUntil.Time.Format(time.RFC3339Nano)
		}
		observability.NoteSwallowed(json.Unmarshal(initJSON, &s.InitialPayload))
		observability.NoteSwallowed(json.Unmarshal(finalJSON, &s.FinalOutput))

		sagas = append(sagas, s)
	}

	return sagas, nil
}

func (a *PostgresSagaRepositoryAdapter) scanSagaWithSteps(ctx context.Context, row *sql.Row, tenantID string) (*sagaContracts.SagaInstance, error) {
	var s sagaContracts.SagaInstance
	var stateStr string
	var initJSON, finalJSON []byte
	var created, updated, deadline time.Time
	var leaseUntil sql.NullTime

	err := row.Scan(
		&s.ID,
		&s.PublicID,
		&s.TenantID,
		&s.WorkflowDefinitionID,
		&s.WorkflowVersion,
		&s.SpecHash,
		&s.Source,
		&s.ExternalID,
		&s.InputHash,
		&s.CorrelationID,
		&stateStr,
		&s.Version,
		&s.FencingToken,
		&s.LeaseOwner,
		&leaseUntil,
		&deadline,
		&initJSON,
		&finalJSON,
		&created,
		&updated,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("saga instance not found")
		}
		return nil, errors.NewInternalError(err.Error())
	}

	s.CurrentState = sagaContracts.SagaState(stateStr)
	s.CreatedAt = created.Format(time.RFC3339Nano)
	s.UpdatedAt = updated.Format(time.RFC3339Nano)
	s.DeadlineAt = deadline.Format(time.RFC3339Nano)
	if leaseUntil.Valid {
		s.LeaseUntil = leaseUntil.Time.Format(time.RFC3339Nano)
	}
	observability.NoteSwallowed(json.Unmarshal(initJSON, &s.InitialPayload))
	observability.NoteSwallowed(json.Unmarshal(finalJSON, &s.FinalOutput))

	stepRows, err := a.db.QueryContext(ctx, queries.SelectSagaSteps, tenantID, s.ID)
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	defer stepRows.Close()

	for stepRows.Next() {
		var step sagaContracts.SagaStepExecution
		var stepStateStr string
		var stepInJSON, stepOutJSON []byte
		var started, completed sql.NullTime
		var stepCreated, stepUpdated time.Time

		err := stepRows.Scan(
			&step.ID,
			&step.PublicID,
			&step.TenantID,
			&step.SagaID,
			&step.StepID,
			&stepStateStr,
			&step.AttemptCount,
			&stepInJSON,
			&stepOutJSON,
			&step.ErrorMessage,
			&step.IdempotencyKey,
			&started,
			&completed,
			&stepCreated,
			&stepUpdated,
		)
		if err != nil {
			return nil, errors.NewInternalError(err.Error())
		}

		step.ExecutionState = sagaContracts.StepExecutionState(stepStateStr)
		step.CreatedAt = stepCreated.Format(time.RFC3339Nano)
		step.UpdatedAt = stepUpdated.Format(time.RFC3339Nano)
		if started.Valid {
			step.StartedAt = started.Time.Format(time.RFC3339Nano)
		}
		if completed.Valid {
			step.CompletedAt = completed.Time.Format(time.RFC3339Nano)
		}
		observability.NoteSwallowed(json.Unmarshal(stepInJSON, &step.StepInput))
		observability.NoteSwallowed(json.Unmarshal(stepOutJSON, &step.StepOutput))

		s.Steps = append(s.Steps, step)
	}

	return &s, nil
}

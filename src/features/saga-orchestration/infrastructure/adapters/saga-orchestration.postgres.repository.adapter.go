package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	ports "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	"distributed-transaction-engine/src/features/saga-orchestration/infrastructure/queries"
	"distributed-transaction-engine/src/shared/errors"
)

type PostgresSagaRepositoryAdapter struct {
	db *sql.DB
}

var _ ports.SagaRepositoryPort = (*PostgresSagaRepositoryAdapter)(nil)

func NewPostgresSagaRepositoryAdapter(db *sql.DB) *PostgresSagaRepositoryAdapter {
	return &PostgresSagaRepositoryAdapter{db: db}
}

func (a *PostgresSagaRepositoryAdapter) CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error {
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
		instance.CorrelationID,
		string(instance.CurrentState),
		instance.Version,
		instance.FencingToken,
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

func (a *PostgresSagaRepositoryAdapter) GetSagaByID(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error) {
	row := a.db.QueryRowContext(ctx, queries.SelectSagaByID, tenantID, id)
	return a.scanSagaWithSteps(ctx, row, tenantID)
}

func (a *PostgresSagaRepositoryAdapter) GetSagaByPublicID(ctx context.Context, tenantID string, publicID string) (*sagaContracts.SagaInstance, error) {
	row := a.db.QueryRowContext(ctx, queries.SelectSagaByPublicID, tenantID, publicID)
	return a.scanSagaWithSteps(ctx, row, tenantID)
}

func (a *PostgresSagaRepositoryAdapter) UpdateSagaState(ctx context.Context, tenantID string, id string, expectedVersion int64, nextState sagaContracts.SagaState) error {
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

func (a *PostgresSagaRepositoryAdapter) UpdateStepExecution(ctx context.Context, tenantID string, sagaID string, step *sagaContracts.SagaStepExecution) error {
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

func (a *PostgresSagaRepositoryAdapter) ListSagas(ctx context.Context, tenantID string, limit int, offset int) ([]sagaContracts.SagaInstance, error) {
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
		var created, updated time.Time

		err := rows.Scan(
			&s.ID,
			&s.PublicID,
			&s.TenantID,
			&s.WorkflowDefinitionID,
			&s.WorkflowVersion,
			&s.SpecHash,
			&s.Source,
			&s.ExternalID,
			&s.CorrelationID,
			&stateStr,
			&s.Version,
			&s.FencingToken,
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
		_ = json.Unmarshal(initJSON, &s.InitialPayload)
		_ = json.Unmarshal(finalJSON, &s.FinalOutput)

		sagas = append(sagas, s)
	}

	return sagas, nil
}

func (a *PostgresSagaRepositoryAdapter) scanSagaWithSteps(ctx context.Context, row *sql.Row, tenantID string) (*sagaContracts.SagaInstance, error) {
	var s sagaContracts.SagaInstance
	var stateStr string
	var initJSON, finalJSON []byte
	var created, updated time.Time

	err := row.Scan(
		&s.ID,
		&s.PublicID,
		&s.TenantID,
		&s.WorkflowDefinitionID,
		&s.WorkflowVersion,
		&s.SpecHash,
		&s.Source,
		&s.ExternalID,
		&s.CorrelationID,
		&stateStr,
		&s.Version,
		&s.FencingToken,
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
	_ = json.Unmarshal(initJSON, &s.InitialPayload)
	_ = json.Unmarshal(finalJSON, &s.FinalOutput)

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
		_ = json.Unmarshal(stepInJSON, &step.StepInput)
		_ = json.Unmarshal(stepOutJSON, &step.StepOutput)

		s.Steps = append(s.Steps, step)
	}

	return &s, nil
}

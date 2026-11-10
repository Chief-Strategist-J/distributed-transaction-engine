package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	queries "distributed-transaction-engine/src/features/compensation-engine/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"encoding/json"
)

type PostgresCompensationRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresCompensationRepositoryAdapter(db *sql.DB) *PostgresCompensationRepositoryAdapter {
	return &PostgresCompensationRepositoryAdapter{db: db}
}

func (a *PostgresCompensationRepositoryAdapter) CreateCompensationPlanAtomic(
	ctx context.Context,
	plan *contracts.SagaCompensationPlanInstance,
	steps []contracts.SagaStepCompensationInstance,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewInternalError("failed to begin atomic compensation plan tx: " + err.Error())
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		queries.FLOW_CREATE_COMPENSATION_PLAN,
		plan.ID,
		plan.PublicID,
		plan.TenantID,
		plan.SagaID,
		plan.CurrentState,
		plan.TotalSteps,
	)
	if err != nil {
		return errors.NewConflictError("failed to create compensation plan: duplicate plan for saga or storage error")
	}

	for _, step := range steps {
		compPayloadBytes, _ := json.Marshal(step.CompensationPayload)
		resPayloadBytes, _ := json.Marshal(step.ResidualPayload)

		_, err = tx.ExecContext(
			ctx,
			queries.FLOW_INSERT_STEP_COMPENSATION,
			step.ID,
			step.PublicID,
			step.TenantID,
			step.PlanID,
			step.SagaID,
			step.StepID,
			step.StepName,
			step.ExecutionOrder,
			step.Status,
			step.ForwardExecutionID,
			step.CompensationAction,
			string(compPayloadBytes),
			string(resPayloadBytes),
			step.IdempotencyKey,
		)
		if err != nil {
			return errors.NewConflictError("failed to insert step compensation: " + err.Error())
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.NewInternalError("failed to commit compensation plan tx: " + err.Error())
	}
	return nil
}

func (a *PostgresCompensationRepositoryAdapter) GetCompensationPlan(
	ctx context.Context,
	tenantID string,
	sagaID string,
) (spanResult0 *contracts.SagaCompensationPlanInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_COMPENSATION_PLAN, tenantID, sagaID)

	var p contracts.SagaCompensationPlanInstance
	var residualsBytes []byte
	var completedAt sql.NullTime

	err := row.Scan(
		&p.ID,
		&p.PublicID,
		&p.TenantID,
		&p.SagaID,
		&p.CurrentState,
		&p.TotalSteps,
		&p.CompensatedSteps,
		&p.FailedSteps,
		&residualsBytes,
		&p.Version,
		&p.CreatedAt,
		&p.UpdatedAt,
		&completedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("compensation plan not found for saga")
		}
		return nil, errors.NewInternalError("failed to query compensation plan: " + err.Error())
	}
	observability.NoteSwallowed(json.Unmarshal(residualsBytes, &p.Residuals))
	if completedAt.Valid {
		p.CompletedAt = &completedAt.Time
	}

	stepRows, err := a.db.QueryContext(ctx, queries.FLOW_GET_STEP_COMPENSATIONS, tenantID, p.ID)
	if err != nil {
		return nil, errors.NewInternalError("failed to query step compensations: " + err.Error())
	}
	defer stepRows.Close()

	for stepRows.Next() {
		var s contracts.SagaStepCompensationInstance
		var compBytes, resBytes []byte
		var lastErr sql.NullString
		var compAt sql.NullTime

		err := stepRows.Scan(
			&s.ID,
			&s.PublicID,
			&s.TenantID,
			&s.PlanID,
			&s.SagaID,
			&s.StepID,
			&s.StepName,
			&s.ExecutionOrder,
			&s.Status,
			&s.ForwardExecutionID,
			&s.CompensationAction,
			&compBytes,
			&resBytes,
			&s.IdempotencyKey,
			&s.AttemptCount,
			&lastErr,
			&s.Version,
			&s.CreatedAt,
			&s.UpdatedAt,
			&compAt,
		)
		if err != nil {
			return nil, errors.NewInternalError("failed to scan step compensation: " + err.Error())
		}
		observability.NoteSwallowed(json.Unmarshal(compBytes, &s.CompensationPayload))
		observability.NoteSwallowed(json.Unmarshal(resBytes, &s.ResidualPayload))
		if lastErr.Valid {
			s.LastError = lastErr.String
		}
		if compAt.Valid {
			s.CompensatedAt = &compAt.Time
		}
		p.Steps = append(p.Steps, s)
	}

	return &p, nil
}

func (a *PostgresCompensationRepositoryAdapter) UpdateStepOutcome(
	ctx context.Context,
	tenantID string,
	planID string,
	stepID string,
	outcome contracts.StepCompensationStatus,
	residual map[string]interface{},
	errorMessage string,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	resBytes, _ := json.Marshal(residual)
	if len(residual) == 0 {
		resBytes = []byte("{}")
	}

	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_UPDATE_STEP_COMPENSATION_OUTCOME,
		outcome,
		string(resBytes),
		errorMessage,
		tenantID,
		planID,
		stepID,
	)
	if err != nil {
		return errors.NewInternalError("failed to update step compensation outcome: " + err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError("failed to inspect rows affected")
	}
	if rows == 0 {
		return errors.NewNotFoundError("step compensation not found")
	}
	return nil
}

func (a *PostgresCompensationRepositoryAdapter) UpdatePlanCAS(
	ctx context.Context,
	tenantID string,
	planID string,
	expectedVersion int64,
	newState contracts.CompensationPlanState,
	compensatedSteps int,
	failedSteps int,
	residuals []map[string]interface{},
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	resBytes, _ := json.Marshal(residuals)
	if len(residuals) == 0 {
		resBytes = []byte("[]")
	}

	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_UPDATE_COMPENSATION_PLAN_CAS,
		newState,
		compensatedSteps,
		failedSteps,
		string(resBytes),
		tenantID,
		planID,
		expectedVersion,
	)
	if err != nil {
		return errors.NewInternalError("failed to update compensation plan: " + err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError("failed to inspect rows affected")
	}
	if rows == 0 {
		return errors.NewConflictError("stale version on compensation plan CAS update")
	}
	return nil
}

package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	queries "distributed-transaction-engine/src/features/sequential-execution/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"encoding/json"
)

type PostgresSequentialExecutionRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresSequentialExecutionRepositoryAdapter(db *sql.DB) *PostgresSequentialExecutionRepositoryAdapter {
	return &PostgresSequentialExecutionRepositoryAdapter{db: db}
}

func (a *PostgresSequentialExecutionRepositoryAdapter) CreateSequentialSaga(ctx context.Context, saga *contracts.SequentialSagaState) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	outputBytes, err := json.Marshal(saga.FinalOutput)
	if err != nil {
		return errors.NewInternalError("failed to serialize final output")
	}

	_, err = a.db.ExecContext(
		ctx,
		queries.FLOW_CREATE_SEQUENTIAL_SAGA,
		saga.ID, saga.PublicID, saga.TenantID, saga.WorkflowDefinitionID, 1,
		"spec_hash_placeholder", "SYSTEM", saga.ID, "input_hash_placeholder", saga.ID,
		saga.CurrentState, saga.Version, saga.CurrentStepIndex, saga.TotalSteps, string(saga.Phase),
		saga.PivotReached, saga.OwnerEpoch, saga.CreatedAt.Add(24*3600*1e9), "{}", string(outputBytes),
		saga.CreatedAt, saga.UpdatedAt,
	)
	if err != nil {
		return errors.NewConflictError("failed to create saga instance: duplicate or storage error")
	}
	return nil
}

func (a *PostgresSequentialExecutionRepositoryAdapter) GetSequentialSaga(ctx context.Context, tenantID string, sagaID string) (spanResult0 *contracts.SequentialSagaState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_SEQUENTIAL_SAGA, tenantID, sagaID)

	var s contracts.SequentialSagaState
	var phaseStr string
	var outputBytes []byte

	err := row.Scan(
		&s.ID, &s.PublicID, &s.TenantID, &s.WorkflowDefinitionID, &s.CurrentState,
		&s.CurrentStepIndex, &s.TotalSteps, &phaseStr, &s.PivotReached,
		&s.Version, &s.OwnerEpoch, &outputBytes, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("saga instance not found")
		}
		return nil, errors.NewInternalError("database query failed")
	}

	s.Phase = contracts.ExecutionPhase(phaseStr)
	s.FinalOutput = make(map[string]interface{})
	observability.NoteSwallowed(json.Unmarshal(outputBytes, &s.FinalOutput))
	return &s, nil
}

func (a *PostgresSequentialExecutionRepositoryAdapter) UpdateSagaCAS(
	ctx context.Context,
	tenantID string,
	sagaID string,
	expectedVersion int64,
	ownerEpoch int64,
	newState string,
	nextStepIndex int,
	nextPhase contracts.ExecutionPhase,
	pivotReached bool,
	output map[string]interface{},
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	outputBytes, err := json.Marshal(output)
	if err != nil {
		return errors.NewInternalError("failed to marshal output")
	}

	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_UPDATE_SAGA_CAS,
		newState, nextStepIndex, string(nextPhase), pivotReached, string(outputBytes),
		tenantID, sagaID, expectedVersion, ownerEpoch,
	)
	if err != nil {
		return errors.NewInternalError("database update execution failed")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError("failed to inspect rows affected")
	}
	if rows == 0 {
		return errors.NewConflictError("optimistic lock or owner epoch conflict during CAS update")
	}
	return nil
}

func (a *PostgresSequentialExecutionRepositoryAdapter) SaveContextEntry(ctx context.Context, entry *contracts.StepContextEntry) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	valBytes, err := json.Marshal(entry.EntryValue)
	if err != nil {
		return errors.NewInternalError("failed to serialize context value")
	}

	_, err = a.db.ExecContext(
		ctx,
		queries.FLOW_INSERT_CONTEXT_ENTRY,
		entry.ID, entry.PublicID, entry.TenantID, entry.SagaID, entry.ProducerStepIndex,
		entry.EntryName, string(valBytes), entry.CorrelationID, entry.ID,
	)
	if err != nil {
		return errors.NewConflictError("duplicate context entry")
	}
	return nil
}

func (a *PostgresSequentialExecutionRepositoryAdapter) GetContextEntries(ctx context.Context, tenantID string, sagaID string) (spanResult0 []contracts.StepContextEntry, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_CONTEXT_ENTRIES, tenantID, sagaID)
	if err != nil {
		return nil, errors.NewInternalError("failed to query context entries")
	}
	defer rows.Close()

	var entries []contracts.StepContextEntry
	for rows.Next() {
		var e contracts.StepContextEntry
		var valBytes []byte
		if scanErr := rows.Scan(&e.ID, &e.PublicID, &e.TenantID, &e.SagaID, &e.ProducerStepIndex, &e.EntryName, &valBytes, &e.CorrelationID); scanErr != nil {
			return nil, errors.NewInternalError("failed to scan context entry")
		}
		e.EntryValue = make(map[string]interface{})
		observability.NoteSwallowed(json.Unmarshal(valBytes, &e.EntryValue))
		entries = append(entries, e)
	}
	return entries, nil
}

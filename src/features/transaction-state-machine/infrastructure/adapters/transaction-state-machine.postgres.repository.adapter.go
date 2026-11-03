package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	queries "distributed-transaction-engine/src/features/transaction-state-machine/infrastructure/queries"
	"distributed-transaction-engine/src/shared/errors"
)

type PostgresTransactionStateMachineRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresTransactionStateMachineRepositoryAdapter(db *sql.DB) *PostgresTransactionStateMachineRepositoryAdapter {
	return &PostgresTransactionStateMachineRepositoryAdapter{db: db}
}

func (a *PostgresTransactionStateMachineRepositoryAdapter) GetState(ctx context.Context, tenantID string, sagaID string) (*contracts.SagaStateRecord, error) {
	row := a.db.QueryRowContext(ctx, queries.FlowSelectSagaState, tenantID, sagaID)
	var record contracts.SagaStateRecord
	var currentState string
	var createdAt, updatedAt time.Time

	err := row.Scan(
		&record.ID,
		&record.PublicID,
		&record.TenantID,
		&record.Source,
		&record.ExternalID,
		&record.CorrelationID,
		&currentState,
		&record.Version,
		&record.FencingToken,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("saga state record not found")
		}
		return nil, a.mapDbError(err)
	}

	record.CurrentState = contracts.TransactionState(currentState)
	record.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	record.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
	return &record, nil
}

func (a *PostgresTransactionStateMachineRepositoryAdapter) CreateState(ctx context.Context, record *contracts.SagaStateRecord) error {
	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.FlowInsertSagaState,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.Source,
		record.ExternalID,
		record.CorrelationID,
		string(record.CurrentState),
		record.Version,
		record.FencingToken,
		now,
		now,
	)
	if err != nil {
		return a.mapDbError(err)
	}
	return nil
}

func (a *PostgresTransactionStateMachineRepositoryAdapter) UpdateStateCAS(
	ctx context.Context,
	tenantID string,
	sagaID string,
	fromState contracts.TransactionState,
	toState contracts.TransactionState,
	expectedVersion int64,
	fencingToken int64,
	reason string,
) (*contracts.SagaStateRecord, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.NewInternalError("failed to initiate db transaction: " + err.Error())
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	row := tx.QueryRowContext(
		ctx,
		queries.FlowUpdateSagaStateCAS,
		string(toState),
		fencingToken,
		now,
		tenantID,
		sagaID,
		expectedVersion,
		string(fromState),
	)

	var record contracts.SagaStateRecord
	var currentState string
	var createdAt, updatedAt time.Time
	err = row.Scan(
		&record.ID,
		&record.PublicID,
		&record.TenantID,
		&record.Source,
		&record.ExternalID,
		&record.CorrelationID,
		&currentState,
		&record.Version,
		&record.FencingToken,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewConflictError("optimistic concurrency conflict: state or version mismatch")
		}
		return nil, a.mapDbError(err)
	}

	auditPublicID := fmt.Sprintf("aud_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26])
	auditID := uuid.New().String()
	_, err = tx.ExecContext(
		ctx,
		queries.FlowInsertStateAudit,
		auditID,
		auditPublicID,
		tenantID,
		sagaID,
		record.CorrelationID,
		string(fromState),
		string(toState),
		expectedVersion,
		record.Version,
		fencingToken,
		reason,
		now,
		now,
	)
	if err != nil {
		return nil, a.mapDbError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.NewInternalError("failed to commit transaction: " + err.Error())
	}

	record.CurrentState = contracts.TransactionState(currentState)
	record.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	record.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
	return &record, nil
}

func (a *PostgresTransactionStateMachineRepositoryAdapter) GetTransitionAuditLogs(ctx context.Context, tenantID string, sagaID string) ([]contracts.StateTransitionAuditRecord, error) {
	rows, err := a.db.QueryContext(ctx, queries.FlowSelectStateAudits, tenantID, sagaID)
	if err != nil {
		return nil, a.mapDbError(err)
	}
	defer rows.Close()

	var logs []contracts.StateTransitionAuditRecord
	for rows.Next() {
		var log contracts.StateTransitionAuditRecord
		var fromState, toState string
		var createdAt, updatedAt time.Time

		err := rows.Scan(
			&log.ID,
			&log.PublicID,
			&log.TenantID,
			&log.SagaID,
			&log.CorrelationID,
			&fromState,
			&toState,
			&log.VersionBefore,
			&log.VersionAfter,
			&log.FencingToken,
			&log.Reason,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, errors.NewInternalError("failed to scan audit row: " + err.Error())
		}

		log.FromState = contracts.TransactionState(fromState)
		log.ToState = contracts.TransactionState(toState)
		log.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		log.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
		logs = append(logs, log)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewInternalError("audit rows iteration error: " + err.Error())
	}

	return logs, nil
}

func (a *PostgresTransactionStateMachineRepositoryAdapter) mapDbError(err error) error {
	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "saga_instance_states_public_id_uk"):
		return errors.NewConflictError("duplicate public_id constraint violation")
	case strings.Contains(errMsg, "saga_instance_states_external_uk"):
		return errors.NewConflictError("duplicate natural external key for tenant and source")
	case strings.Contains(errMsg, "saga_instance_states_current_state_ck"):
		return errors.NewValidationError("illegal transaction current_state value")
	case strings.Contains(errMsg, "fk_state_transition_audit_saga"):
		return errors.NewValidationError("referenced saga instance does not exist in state table")
	default:
		return errors.NewInternalError("database operation error: " + errMsg)
	}
}

package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/features/manual-intervention/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type PostgresManualInterventionRepositoryAdapter struct {
	db DBExecutor
}

func NewPostgresManualInterventionRepositoryAdapter(db DBExecutor) *PostgresManualInterventionRepositoryAdapter {
	return &PostgresManualInterventionRepositoryAdapter{db: db}
}

func (a *PostgresManualInterventionRepositoryAdapter) SaveAuditRecord(ctx context.Context, record contracts.ManualInterventionAuditRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_INSERT_MANUAL_INTERVENTION_AUDIT,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.Source,
		record.ExternalID,
		record.CorrelationID,
		record.SagaID,
		record.OperatorID,
		record.OperatorRole,
		record.ActionType,
		record.Reason,
		record.ApprovalTicketID,
		record.SyntheticPayload,
		record.OldState,
		record.NewState,
		record.Version,
		record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresManualInterventionRepositoryAdapter) ListAuditRecordsBySaga(ctx context.Context, tenantID string, sagaID uuid.UUID) (spanResult0 []contracts.ManualInterventionAuditRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_SELECT_MANUAL_INTERVENTIONS_BY_SAGA, tenantID, sagaID)
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}
	defer rows.Close()

	return a.scanAuditRows(rows)
}

func (a *PostgresManualInterventionRepositoryAdapter) ListAuditRecordsByOperator(ctx context.Context, tenantID string, operatorID string) (spanResult0 []contracts.ManualInterventionAuditRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_SELECT_MANUAL_INTERVENTIONS_BY_OPERATOR, tenantID, operatorID)
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}
	defer rows.Close()

	return a.scanAuditRows(rows)
}

func (a *PostgresManualInterventionRepositoryAdapter) scanAuditRows(rows *sql.Rows) ([]contracts.ManualInterventionAuditRecord, error) {
	var results []contracts.ManualInterventionAuditRecord
	for rows.Next() {
		var rec contracts.ManualInterventionAuditRecord
		err := rows.Scan(
			&rec.ID,
			&rec.PublicID,
			&rec.TenantID,
			&rec.Source,
			&rec.ExternalID,
			&rec.CorrelationID,
			&rec.SagaID,
			&rec.OperatorID,
			&rec.OperatorRole,
			&rec.ActionType,
			&rec.Reason,
			&rec.ApprovalTicketID,
			&rec.SyntheticPayload,
			&rec.OldState,
			&rec.NewState,
			&rec.Version,
			&rec.CreatedAt,
		)
		if err != nil {
			return nil, appErrors.NewInternalError(err.Error())
		}
		results = append(results, rec)
	}
	return results, nil
}

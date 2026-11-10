package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/features/reconciliation-engine/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type PostgresReconciliationRepositoryAdapter struct {
	db DBExecutor
}

func NewPostgresReconciliationRepositoryAdapter(db DBExecutor) *PostgresReconciliationRepositoryAdapter {
	return &PostgresReconciliationRepositoryAdapter{db: db}
}

func (a *PostgresReconciliationRepositoryAdapter) SaveAnomaly(ctx context.Context, anomaly contracts.ReconciliationAnomalyRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_INSERT_RECONCILIATION_ANOMALY,
		anomaly.ID,
		anomaly.PublicID,
		anomaly.TenantID,
		anomaly.Source,
		anomaly.ExternalID,
		anomaly.CorrelationID,
		anomaly.SagaID,
		anomaly.AnomalyType,
		anomaly.Status,
		anomaly.InternalStatus,
		anomaly.ExternalStatus,
		anomaly.InternalAmountCents,
		anomaly.ExternalAmountCents,
		anomaly.Currency,
		anomaly.DiscrepancyReason,
		anomaly.ResolutionAction,
		anomaly.Version,
		anomaly.CreatedAt,
		anomaly.UpdatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresReconciliationRepositoryAdapter) GetAnomalyBySaga(ctx context.Context, tenantID string, sagaID uuid.UUID) (spanResult0 *contracts.ReconciliationAnomalyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_SELECT_RECONCILIATION_ANOMALY_BY_SAGA, tenantID, sagaID)
	rec, err := a.scanAnomalyRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, appErrors.NewNotFoundError("Anomaly not found for saga")
		}
		return nil, appErrors.NewInternalError(err.Error())
	}
	return rec, nil
}

func (a *PostgresReconciliationRepositoryAdapter) ListAnomalies(ctx context.Context, tenantID string, status *contracts.ReconciliationStatus) (spanResult0 []contracts.ReconciliationAnomalyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.queryAnomaliesRows(ctx, tenantID, status)
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}
	defer rows.Close()

	var results []contracts.ReconciliationAnomalyRecord
	for rows.Next() {
		rec, scanErr := a.scanAnomalyRows(rows)
		if scanErr != nil {
			return nil, appErrors.NewInternalError(scanErr.Error())
		}
		results = append(results, *rec)
	}
	return results, nil
}

func (a *PostgresReconciliationRepositoryAdapter) queryAnomaliesRows(ctx context.Context, tenantID string, status *contracts.ReconciliationStatus) (*sql.Rows, error) {
	if status != nil {
		return a.db.QueryContext(ctx, queries.FLOW_SELECT_RECONCILIATION_ANOMALIES_BY_TENANT_AND_STATUS, tenantID, *status)
	}
	return a.db.QueryContext(ctx, queries.FLOW_SELECT_RECONCILIATION_ANOMALIES_BY_TENANT, tenantID)
}

func (a *PostgresReconciliationRepositoryAdapter) scanAnomalyRow(row *sql.Row) (*contracts.ReconciliationAnomalyRecord, error) {
	var rec contracts.ReconciliationAnomalyRecord
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.SagaID, &rec.AnomalyType, &rec.Status,
		&rec.InternalStatus, &rec.ExternalStatus, &rec.InternalAmountCents,
		&rec.ExternalAmountCents, &rec.Currency, &rec.DiscrepancyReason,
		&rec.ResolutionAction, &rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
	)
	return &rec, err
}

func (a *PostgresReconciliationRepositoryAdapter) scanAnomalyRows(rows *sql.Rows) (*contracts.ReconciliationAnomalyRecord, error) {
	var rec contracts.ReconciliationAnomalyRecord
	err := rows.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.SagaID, &rec.AnomalyType, &rec.Status,
		&rec.InternalStatus, &rec.ExternalStatus, &rec.InternalAmountCents,
		&rec.ExternalAmountCents, &rec.Currency, &rec.DiscrepancyReason,
		&rec.ResolutionAction, &rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
	)
	return &rec, err
}

func (a *PostgresReconciliationRepositoryAdapter) UpdateAnomalyStatus(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, nextStatus contracts.ReconciliationStatus, resolutionAction string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_RECONCILIATION_ANOMALY_STATUS, nextStatus, resolutionAction, tenantID, id, expectedVersion)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	if rowsAffected == 0 {
		return appErrors.NewConflictError("Version conflict or record not found for update")
	}
	return nil
}

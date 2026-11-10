package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/ports"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type PostgresWorkflowVersioningRepositoryAdapter struct {
	db *sql.DB
}

var _ ports.WorkflowVersioningRepositoryPort = (*PostgresWorkflowVersioningRepositoryAdapter)(nil)

func NewPostgresWorkflowVersioningRepositoryAdapter(db *sql.DB) *PostgresWorkflowVersioningRepositoryAdapter {
	return &PostgresWorkflowVersioningRepositoryAdapter{db: db}
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) InsertBundle(ctx context.Context, bundle *contracts.WorkflowBundleRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	payloadBytes, capBytes, err := a.marshalBundleBlobs(bundle)
	if err != nil {
		return err
	}
	_, err = a.db.ExecContext(ctx, queries.FLOW_INSERT_BUNDLE,
		bundle.ID, bundle.PublicID, bundle.TenantID, bundle.Source, bundle.ExternalID, bundle.CorrelationID,
		bundle.WorkflowName, bundle.VersionTag, bundle.ContentHash, bundle.Signature, string(bundle.Status),
		payloadBytes, capBytes, bundle.CreatedAt, bundle.UpdatedAt,
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return appErrors.NewConflictError("workflow bundle version tag or content hash already exists")
		}
		return appErrors.NewInternalError("failed to insert workflow bundle: " + err.Error())
	}
	return nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) marshalBundleBlobs(bundle *contracts.WorkflowBundleRecord) ([]byte, []byte, error) {
	payloadBytes, err := json.Marshal(bundle.BundlePayload)
	if err != nil {
		return nil, nil, appErrors.NewInternalError("failed to serialize bundle payload: " + err.Error())
	}
	capBytes, err := json.Marshal(bundle.CapabilitiesRequired)
	if err != nil {
		return nil, nil, appErrors.NewInternalError("failed to serialize capabilities: " + err.Error())
	}
	return payloadBytes, capBytes, nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) GetBundleByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.WorkflowBundleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_SELECT_BUNDLE_BY_ID, tenantID, id)
	return a.scanBundleRow(row)
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) GetBundleByVersion(ctx context.Context, tenantID string, workflowName string, versionTag string) (spanResult0 *contracts.WorkflowBundleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_SELECT_BUNDLE_BY_VERSION, tenantID, workflowName, versionTag)
	return a.scanBundleRow(row)
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) GetActiveBundle(ctx context.Context, tenantID string, workflowName string) (spanResult0 *contracts.WorkflowBundleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_SELECT_ACTIVE_BUNDLE, tenantID, workflowName)
	return a.scanBundleRow(row)
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) scanBundleRow(scanner interface{ Scan(...interface{}) error }) (*contracts.WorkflowBundleRecord, error) {
	var b contracts.WorkflowBundleRecord
	var payloadBytes, capBytes []byte
	var statusStr string
	err := scanner.Scan(
		&b.ID, &b.PublicID, &b.TenantID, &b.Source, &b.ExternalID, &b.CorrelationID,
		&b.WorkflowName, &b.VersionTag, &b.ContentHash, &b.Signature, &statusStr,
		&payloadBytes, &capBytes, &b.CreatedAt, &b.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, appErrors.NewNotFoundError("workflow bundle not found")
		}
		return nil, appErrors.NewInternalError("failed to scan workflow bundle: " + err.Error())
	}
	b.Status = contracts.BundleStatus(statusStr)
	if err := a.unmarshalBundleBlobs(&b, payloadBytes, capBytes); err != nil {
		return nil, err
	}
	return &b, nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) unmarshalBundleBlobs(b *contracts.WorkflowBundleRecord, payloadBytes, capBytes []byte) error {
	if err := json.Unmarshal(payloadBytes, &b.BundlePayload); err != nil {
		return appErrors.NewInternalError("failed to unmarshal bundle payload: " + err.Error())
	}
	if err := json.Unmarshal(capBytes, &b.CapabilitiesRequired); err != nil {
		return appErrors.NewInternalError("failed to unmarshal capabilities: " + err.Error())
	}
	return nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) UpdateBundleStatus(ctx context.Context, tenantID string, id uuid.UUID, newStatus contracts.BundleStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_BUNDLE_STATUS, string(newStatus), tenantID, id)
	if err != nil {
		return appErrors.NewInternalError("failed to update bundle status: " + err.Error())
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return appErrors.NewInternalError("failed to read affected rows: " + err.Error())
	}
	if rows == 0 {
		return appErrors.NewNotFoundError("workflow bundle not found")
	}
	return nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) InsertPromotion(ctx context.Context, promo *contracts.WorkflowVersionPromotionRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_INSERT_PROMOTION,
		promo.ID, promo.PublicID, promo.TenantID, promo.Source, promo.ExternalID, promo.CorrelationID,
		promo.BundleID, string(promo.Action), promo.ProposerID, promo.ApproverID, string(promo.Status), promo.Reason,
		promo.DecidedAt, promo.CreatedAt, promo.UpdatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to insert promotion record: " + err.Error())
	}
	return nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) GetPromotionByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.WorkflowVersionPromotionRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_SELECT_PROMOTION_BY_ID, tenantID, id)
	var p contracts.WorkflowVersionPromotionRecord
	var actionStr, statusStr string
	err := row.Scan(
		&p.ID, &p.PublicID, &p.TenantID, &p.Source, &p.ExternalID, &p.CorrelationID,
		&p.BundleID, &actionStr, &p.ProposerID, &p.ApproverID, &statusStr, &p.Reason,
		&p.DecidedAt, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, appErrors.NewNotFoundError("workflow promotion not found")
		}
		return nil, appErrors.NewInternalError("failed to scan promotion: " + err.Error())
	}
	p.Action = contracts.PromotionAction(actionStr)
	p.Status = contracts.PromotionStatus(statusStr)
	return &p, nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) UpdatePromotionDecision(ctx context.Context, input contracts.UpdatePromotionDecisionInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_PROMOTION_DECISION, string(input.Status), input.ApproverID, input.Reason, input.DecidedAt, input.TenantID, input.ID)
	if err != nil {
		return appErrors.NewInternalError("failed to update promotion decision: " + err.Error())
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return appErrors.NewInternalError("failed to read affected rows: " + err.Error())
	}
	if rows == 0 {
		return appErrors.NewNotFoundError("workflow promotion not found")
	}
	return nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) GetEntityCohort(ctx context.Context, tenantID string, workflowName string, entityID string) (spanResult0 *contracts.WorkflowEntityCohortRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_SELECT_ENTITY_COHORT, tenantID, workflowName, entityID)
	var c contracts.WorkflowEntityCohortRecord
	err := row.Scan(
		&c.ID, &c.PublicID, &c.TenantID, &c.Source, &c.ExternalID, &c.CorrelationID,
		&c.WorkflowName, &c.EntityID, &c.CohortBucket, &c.BundleID, &c.AssignedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, appErrors.NewInternalError("failed to scan entity cohort: " + err.Error())
	}
	return &c, nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) InsertEntityCohort(ctx context.Context, cohort *contracts.WorkflowEntityCohortRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_INSERT_ENTITY_COHORT,
		cohort.ID, cohort.PublicID, cohort.TenantID, cohort.Source, cohort.ExternalID, cohort.CorrelationID,
		cohort.WorkflowName, cohort.EntityID, cohort.CohortBucket, cohort.BundleID, cohort.AssignedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to insert entity cohort: " + err.Error())
	}
	return nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) InsertMigration(ctx context.Context, migration *contracts.WorkflowVersionMigrationRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	mapBytes, err := json.Marshal(migration.StateMappingApplied)
	if err != nil {
		return appErrors.NewInternalError("failed to serialize state mapping: " + err.Error())
	}
	_, err = a.db.ExecContext(ctx, queries.FLOW_INSERT_MIGRATION,
		migration.ID, migration.PublicID, migration.TenantID, migration.Source, migration.ExternalID, migration.CorrelationID,
		migration.SagaID, migration.FromBundleID, migration.ToBundleID, string(migration.Status), migration.SafePointStepID,
		mapBytes, migration.CreatedAt, migration.UpdatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to insert migration record: " + err.Error())
	}
	return nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) GetMigrationByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.WorkflowVersionMigrationRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_SELECT_MIGRATION_BY_ID, tenantID, id)
	var m contracts.WorkflowVersionMigrationRecord
	var mapBytes []byte
	var statusStr string
	err := row.Scan(
		&m.ID, &m.PublicID, &m.TenantID, &m.Source, &m.ExternalID, &m.CorrelationID,
		&m.SagaID, &m.FromBundleID, &m.ToBundleID, &statusStr, &m.SafePointStepID,
		&mapBytes, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, appErrors.NewNotFoundError("workflow migration not found")
		}
		return nil, appErrors.NewInternalError("failed to scan migration: " + err.Error())
	}
	m.Status = contracts.MigrationStatus(statusStr)
	if err := json.Unmarshal(mapBytes, &m.StateMappingApplied); err != nil {
		return nil, appErrors.NewInternalError("failed to unmarshal state mapping: " + err.Error())
	}
	return &m, nil
}

func (a *PostgresWorkflowVersioningRepositoryAdapter) UpdateMigrationStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.MigrationStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_MIGRATION_STATUS, string(status), tenantID, id)
	if err != nil {
		return appErrors.NewInternalError("failed to update migration status: " + err.Error())
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return appErrors.NewInternalError("failed to read affected rows: " + err.Error())
	}
	if rows == 0 {
		return appErrors.NewNotFoundError("workflow migration not found")
	}
	return nil
}

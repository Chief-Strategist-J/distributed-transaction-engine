package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/features/operational-dashboard/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"time"
)

type DashboardPostgresRepositoryAdapter struct {
	db *sql.DB
}

func NewDashboardPostgresRepositoryAdapter(db *sql.DB) *DashboardPostgresRepositoryAdapter {
	return &DashboardPostgresRepositoryAdapter{db: db}
}

func (a *DashboardPostgresRepositoryAdapter) CreateStreamSession(ctx context.Context, session *contracts.DashboardStreamSession) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_CREATE_STREAM_SESSION,
		session.ID, session.PublicID, session.TenantID, session.Source, session.ExternalID, session.CorrelationID,
		session.PrincipalID, string(session.ScopeType), session.ImpersonationTicket, session.ImpersonationExpiresAt,
		session.ReauthDeadline, session.LastHeartbeatAt, string(session.Status), session.CreatedAt, session.UpdatedAt, session.Version,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to create dashboard stream session: " + err.Error())
	}
	return nil
}

func (a *DashboardPostgresRepositoryAdapter) GetStreamSession(ctx context.Context, tenantID, sessionID string) (spanResult0 *contracts.DashboardStreamSession, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_STREAM_SESSION, tenantID, sessionID)
	var s contracts.DashboardStreamSession
	var scopeStr, statusStr string

	err := row.Scan(
		&s.ID, &s.PublicID, &s.TenantID, &s.Source, &s.ExternalID, &s.CorrelationID,
		&s.PrincipalID, &scopeStr, &s.ImpersonationTicket, &s.ImpersonationExpiresAt,
		&s.ReauthDeadline, &s.LastHeartbeatAt, &statusStr, &s.CreatedAt, &s.UpdatedAt, &s.Version,
	)
	if err == sql.ErrNoRows {
		return nil, appErrors.NewNotFoundError("stream session not found")
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to query stream session: " + err.Error())
	}
	s.ScopeType = contracts.ScopeType(scopeStr)
	s.Status = contracts.SessionStatus(statusStr)
	return &s, nil
}

func (a *DashboardPostgresRepositoryAdapter) UpdateStreamSessionStatus(ctx context.Context, tenantID, sessionID string, status contracts.SessionStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_STREAM_SESSION_STATUS, tenantID, sessionID, string(status))
	if err != nil {
		return appErrors.NewInternalError("failed to update stream session status: " + err.Error())
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return appErrors.NewNotFoundError("stream session not found")
	}
	return nil
}

func (a *DashboardPostgresRepositoryAdapter) TouchHeartbeat(ctx context.Context, tenantID, sessionID string, lastHeartbeat time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_TOUCH_HEARTBEAT, tenantID, sessionID, lastHeartbeat)
	if err != nil {
		return appErrors.NewInternalError("failed to touch heartbeat: " + err.Error())
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return appErrors.NewNotFoundError("stream session not found")
	}
	return nil
}

func (a *DashboardPostgresRepositoryAdapter) GetTileSnapshots(ctx context.Context, tenantScope string) (spanResult0 []contracts.DashboardTileSnapshot, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_TILE_SNAPSHOTS, tenantScope)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get tile snapshots: " + err.Error())
	}
	defer rows.Close()

	snapshots := make([]contracts.DashboardTileSnapshot, 0)
	for rows.Next() {
		var s contracts.DashboardTileSnapshot
		if err := rows.Scan(
			&s.ID, &s.PublicID, &s.TenantID, &s.Source, &s.ExternalID, &s.CorrelationID,
			&s.TileKey, &s.MetricsPayload, &s.DataAgeMs, &s.IsStale, &s.AsOfVersion,
			&s.SourceTimestamp, &s.CreatedAt, &s.UpdatedAt, &s.Version,
		); err != nil {
			return nil, appErrors.NewInternalError("failed to scan tile snapshot: " + err.Error())
		}
		snapshots = append(snapshots, s)
	}
	return snapshots, nil
}

func (a *DashboardPostgresRepositoryAdapter) UpsertTileSnapshot(ctx context.Context, snapshot *contracts.DashboardTileSnapshot) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_UPSERT_TILE_SNAPSHOT,
		snapshot.ID, snapshot.PublicID, snapshot.TenantID, snapshot.Source, snapshot.ExternalID, snapshot.CorrelationID,
		snapshot.TileKey, snapshot.MetricsPayload, snapshot.DataAgeMs, snapshot.IsStale, snapshot.AsOfVersion,
		snapshot.SourceTimestamp, snapshot.CreatedAt, snapshot.UpdatedAt, snapshot.Version,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to upsert tile snapshot: " + err.Error())
	}
	return nil
}

func (a *DashboardPostgresRepositoryAdapter) CreateActionPreview(ctx context.Context, preview *contracts.DashboardActionPreview) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_CREATE_ACTION_PREVIEW,
		preview.ID, preview.PublicID, preview.TenantID, preview.Source, preview.ExternalID, preview.CorrelationID,
		preview.SagaID, string(preview.ActionType), preview.SnapshotVersion, preview.RequiresSecondApprover,
		preview.MoneyAtStake, preview.PreviewPayload, string(preview.Status), preview.ExpiresAt,
		preview.CreatedAt, preview.UpdatedAt, preview.Version,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to create action preview: " + err.Error())
	}
	return nil
}

func (a *DashboardPostgresRepositoryAdapter) GetActionPreview(ctx context.Context, tenantID, previewID string) (spanResult0 *contracts.DashboardActionPreview, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_ACTION_PREVIEW, tenantID, previewID)
	var p contracts.DashboardActionPreview
	var actStr, statusStr string

	err := row.Scan(
		&p.ID, &p.PublicID, &p.TenantID, &p.Source, &p.ExternalID, &p.CorrelationID,
		&p.SagaID, &actStr, &p.SnapshotVersion, &p.RequiresSecondApprover,
		&p.MoneyAtStake, &p.PreviewPayload, &statusStr, &p.ExpiresAt,
		&p.CreatedAt, &p.UpdatedAt, &p.Version,
	)
	if err == sql.ErrNoRows {
		return nil, appErrors.NewNotFoundError("action preview not found")
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get action preview: " + err.Error())
	}
	p.ActionType = contracts.ActionType(actStr)
	p.Status = contracts.ActionStatus(statusStr)
	return &p, nil
}

func (a *DashboardPostgresRepositoryAdapter) UpdateActionPreviewStatus(ctx context.Context, tenantID, previewID string, status contracts.ActionStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_ACTION_PREVIEW_STATUS, tenantID, previewID, string(status))
	if err != nil {
		return appErrors.NewInternalError("failed to update action preview status: " + err.Error())
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return appErrors.NewNotFoundError("action preview not found")
	}
	return nil
}

func (a *DashboardPostgresRepositoryAdapter) RecordAuditEvent(ctx context.Context, audit *contracts.DashboardAuditEvent) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_RECORD_AUDIT_EVENT,
		audit.ID, audit.PublicID, audit.TenantID, audit.Source, audit.ExternalID, audit.CorrelationID,
		audit.OperatorID, audit.EventType, audit.ResourceID, audit.Details, audit.IPAddress,
		audit.CreatedAt, audit.UpdatedAt, audit.Version,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to record audit event: " + err.Error())
	}
	return nil
}

func (a *DashboardPostgresRepositoryAdapter) GetSagaStateSnapshot(ctx context.Context, tenantID, sagaID string) (spanResult0 int64, spanResult1 contracts.VisualState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, `SELECT version, current_state FROM transaction_state_records WHERE tenant_id = $1 AND saga_id = $2;`, tenantID, sagaID)
	var version int64
	var stateStr string
	if err := row.Scan(&version, &stateStr); err != nil {
		if err == sql.ErrNoRows {
			return 1, contracts.VisualStateRunning, nil
		}
		return 0, "", appErrors.NewInternalError("failed to read saga state: " + err.Error())
	}
	return version, contracts.VisualState(stateStr), nil
}

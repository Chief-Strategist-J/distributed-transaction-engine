package ports

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
)

type DashboardRepositoryPort interface {
	CreateStreamSession(ctx context.Context, session *contracts.DashboardStreamSession) error
	GetStreamSession(ctx context.Context, tenantID, sessionID string) (*contracts.DashboardStreamSession, error)
	UpdateStreamSessionStatus(ctx context.Context, tenantID, sessionID string, status contracts.SessionStatus) error
	TouchHeartbeat(ctx context.Context, tenantID, sessionID string, lastHeartbeat time.Time) error

	GetTileSnapshots(ctx context.Context, tenantScope string) ([]contracts.DashboardTileSnapshot, error)
	UpsertTileSnapshot(ctx context.Context, snapshot *contracts.DashboardTileSnapshot) error

	CreateActionPreview(ctx context.Context, preview *contracts.DashboardActionPreview) error
	GetActionPreview(ctx context.Context, tenantID, previewID string) (*contracts.DashboardActionPreview, error)
	UpdateActionPreviewStatus(ctx context.Context, tenantID, previewID string, status contracts.ActionStatus) error

	RecordAuditEvent(ctx context.Context, audit *contracts.DashboardAuditEvent) error
	GetSagaStateSnapshot(ctx context.Context, tenantID, sagaID string) (int64, contracts.VisualState, error)
}


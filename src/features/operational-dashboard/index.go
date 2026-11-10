package dashboard

import (
	"database/sql"

	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/features/operational-dashboard/infrastructure/adapters"
	"distributed-transaction-engine/src/features/operational-dashboard/infrastructure/ports"
	"distributed-transaction-engine/src/features/operational-dashboard/service"
)

type DashboardService = service.DashboardService
type DashboardServiceDeps = service.DashboardServiceDeps
type DashboardRepositoryPort = ports.DashboardRepositoryPort

type ScopeType = contracts.ScopeType
type SessionStatus = contracts.SessionStatus
type VisualState = contracts.VisualState
type ActionType = contracts.ActionType
type ActionStatus = contracts.ActionStatus

type DashboardStreamSession = contracts.DashboardStreamSession
type DashboardTileSnapshot = contracts.DashboardTileSnapshot
type DashboardActionPreview = contracts.DashboardActionPreview
type DashboardAuditEvent = contracts.DashboardAuditEvent
type StreamEvent = contracts.StreamEvent
type ConnectStreamRequest = contracts.ConnectStreamRequest
type OverviewResponse = contracts.OverviewResponse
type PreviewActionRequest = contracts.PreviewActionRequest
type PreviewActionResponse = contracts.PreviewActionResponse
type ExecuteActionRequest = contracts.ExecuteActionRequest
type ExecuteActionResponse = contracts.ExecuteActionResponse

func NewService(deps DashboardServiceDeps) *DashboardService {
	return service.NewDashboardService(deps)
}

func NewMemoryRepository() ports.DashboardRepositoryPort {
	return adapters.NewDashboardMemoryRepositoryAdapter()
}

func NewPostgresRepository(db *sql.DB) ports.DashboardRepositoryPort {
	return adapters.NewDashboardPostgresRepositoryAdapter(db)
}

func NewDefaultService(repo ports.DashboardRepositoryPort) *DashboardService {
	return service.NewDashboardService(service.DashboardServiceDeps{
		Repo: repo,
	})
}


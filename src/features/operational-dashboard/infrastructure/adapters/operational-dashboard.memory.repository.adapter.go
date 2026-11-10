package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"sync"
	"time"
)

type DashboardMemoryRepositoryAdapter struct {
	mu         sync.RWMutex
	sessions   map[string]*contracts.DashboardStreamSession
	snapshots  map[string]*contracts.DashboardTileSnapshot
	previews   map[string]*contracts.DashboardActionPreview
	audits     []*contracts.DashboardAuditEvent
	sagaStates map[string]struct {
		Version int64
		State   contracts.VisualState
	}
}

func NewDashboardMemoryRepositoryAdapter() *DashboardMemoryRepositoryAdapter {
	return &DashboardMemoryRepositoryAdapter{
		sessions:  make(map[string]*contracts.DashboardStreamSession),
		snapshots: make(map[string]*contracts.DashboardTileSnapshot),
		previews:  make(map[string]*contracts.DashboardActionPreview),
		audits:    make([]*contracts.DashboardAuditEvent, 0),
		sagaStates: make(map[string]struct {
			Version int64
			State   contracts.VisualState
		}),
	}
}

func (a *DashboardMemoryRepositoryAdapter) CreateStreamSession(ctx context.Context, session *contracts.DashboardStreamSession) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := session.TenantID + ":" + session.PublicID
	a.sessions[key] = session
	return nil
}

func (a *DashboardMemoryRepositoryAdapter) GetStreamSession(ctx context.Context, tenantID, sessionID string) (spanResult0 *contracts.DashboardStreamSession, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, s := range a.sessions {
		if s.TenantID == tenantID && (s.PublicID == sessionID || s.ID.String() == sessionID) {
			copied := *s
			return &copied, nil
		}
	}
	return nil, appErrors.NewNotFoundError("stream session not found")
}

func (a *DashboardMemoryRepositoryAdapter) UpdateStreamSessionStatus(ctx context.Context, tenantID, sessionID string, status contracts.SessionStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, s := range a.sessions {
		if s.TenantID == tenantID && (s.PublicID == sessionID || s.ID.String() == sessionID) {
			s.Status = status
			s.UpdatedAt = time.Now().UTC()
			s.Version++
			return nil
		}
	}
	return appErrors.NewNotFoundError("stream session not found")
}

func (a *DashboardMemoryRepositoryAdapter) TouchHeartbeat(ctx context.Context, tenantID, sessionID string, lastHeartbeat time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, s := range a.sessions {
		if s.TenantID == tenantID && (s.PublicID == sessionID || s.ID.String() == sessionID) {
			s.LastHeartbeatAt = lastHeartbeat
			s.UpdatedAt = time.Now().UTC()
			return nil
		}
	}
	return appErrors.NewNotFoundError("stream session not found")
}

func (a *DashboardMemoryRepositoryAdapter) GetTileSnapshots(ctx context.Context, tenantScope string) (spanResult0 []contracts.DashboardTileSnapshot, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	results := make([]contracts.DashboardTileSnapshot, 0)
	for _, s := range a.snapshots {
		if s.TenantID == tenantScope || s.TenantID == "PLATFORM_WIDE" || tenantScope == "PLATFORM_WIDE" {
			results = append(results, *s)
		}
	}
	return results, nil
}

func (a *DashboardMemoryRepositoryAdapter) UpsertTileSnapshot(ctx context.Context, snapshot *contracts.DashboardTileSnapshot) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := snapshot.TenantID + ":" + snapshot.TileKey
	a.snapshots[key] = snapshot
	return nil
}

func (a *DashboardMemoryRepositoryAdapter) CreateActionPreview(ctx context.Context, preview *contracts.DashboardActionPreview) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := preview.TenantID + ":" + preview.PublicID
	a.previews[key] = preview
	return nil
}

func (a *DashboardMemoryRepositoryAdapter) GetActionPreview(ctx context.Context, tenantID, previewID string) (spanResult0 *contracts.DashboardActionPreview, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, p := range a.previews {
		if p.TenantID == tenantID && (p.PublicID == previewID || p.ID.String() == previewID) {
			copied := *p
			return &copied, nil
		}
	}
	return nil, appErrors.NewNotFoundError("action preview not found")
}

func (a *DashboardMemoryRepositoryAdapter) UpdateActionPreviewStatus(ctx context.Context, tenantID, previewID string, status contracts.ActionStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range a.previews {
		if p.TenantID == tenantID && (p.PublicID == previewID || p.ID.String() == previewID) {
			p.Status = status
			p.UpdatedAt = time.Now().UTC()
			p.Version++
			return nil
		}
	}
	return appErrors.NewNotFoundError("action preview not found")
}

func (a *DashboardMemoryRepositoryAdapter) RecordAuditEvent(ctx context.Context, audit *contracts.DashboardAuditEvent) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.audits = append(a.audits, audit)
	return nil
}

func (a *DashboardMemoryRepositoryAdapter) SetSagaState(tenantID, sagaID string, version int64, state contracts.VisualState) {
	defer observability.TraceEnter(nil)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sagaStates[tenantID+":"+sagaID] = struct {
		Version int64
		State   contracts.VisualState
	}{Version: version, State: state}
}

func (a *DashboardMemoryRepositoryAdapter) GetSagaStateSnapshot(ctx context.Context, tenantID, sagaID string) (spanResult0 int64, spanResult1 contracts.VisualState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	entry, ok := a.sagaStates[tenantID+":"+sagaID]
	if !ok {
		return 1, contracts.VisualStateRunning, nil
	}
	return entry.Version, entry.State, nil
}

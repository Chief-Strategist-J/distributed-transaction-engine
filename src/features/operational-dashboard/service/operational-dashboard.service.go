package service

import (
	"context"
	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/features/operational-dashboard/domain/machines"
	"distributed-transaction-engine/src/features/operational-dashboard/domain/rules"
	"distributed-transaction-engine/src/features/operational-dashboard/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"sync"
	"time"
)

type DashboardServiceDeps struct {
	Repo ports.DashboardRepositoryPort
}

type clientStream struct {
	SessionID   string
	TenantScope string
	Ch          chan contracts.StreamEvent
}

type DashboardService struct {
	repo         ports.DashboardRepositoryPort
	stateMachine *machines.StreamSessionStateMachine
	mu           sync.RWMutex
	clients      map[string]*clientStream
}

func NewDashboardService(deps DashboardServiceDeps) *DashboardService {
	return &DashboardService{
		repo:         deps.Repo,
		stateMachine: machines.NewStreamSessionStateMachine(),
		clients:      make(map[string]*clientStream),
	}
}

func (s *DashboardService) GetOverview(ctx context.Context, principalTenant string, req *contracts.ConnectStreamRequest, impersonationTenant *string, clientIP string) (spanResult0 *contracts.OverviewResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tenantScope, err := rules.ResolveStreamScope(principalTenant, req, impersonationTenant)
	if err != nil {
		return nil, err
	}

	tiles, err := s.repo.GetTileSnapshots(ctx, tenantScope)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	hasStale := false
	var maxAge int64

	for i := range tiles {
		rules.SanitizeTileFreshness(&tiles[i], now)
		if tiles[i].IsStale {
			hasStale = true
		}
		if tiles[i].DataAgeMs > maxAge {
			maxAge = tiles[i].DataAgeMs
		}
	}

	banner := ""
	if hasStale {
		banner = "WARNING: Metrics data sources degraded or stale. Action controls restricted."
	}

	auditID := uuid.New()
	pubAuditID := fmt.Sprintf("dae_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26])
	audit := &contracts.DashboardAuditEvent{
		ID:            auditID,
		PublicID:      pubAuditID,
		TenantID:      tenantScope,
		Source:        "DASHBOARD_GATEWAY",
		ExternalID:    fmt.Sprintf("ovw_%d", now.UnixNano()),
		CorrelationID: auditID,
		OperatorID:    req.PrincipalID,
		EventType:     "OVERVIEW_ACCESSED",
		ResourceID:    tenantScope,
		Details:       json.RawMessage(`{"action":"get_overview"}`),
		IPAddress:     clientIP,
		CreatedAt:     now,
		UpdatedAt:     now,
		Version:       1,
	}
	observability.NoteSwallowed(s.repo.RecordAuditEvent(ctx, audit))

	return &contracts.OverviewResponse{
		TenantScope:   tenantScope,
		Tiles:         tiles,
		OverallBanner: banner,
		DataAgeMs:     maxAge,
		Timestamp:     now,
	}, nil
}

func (s *DashboardService) RegisterStreamSession(ctx context.Context, principalTenant string, req *contracts.ConnectStreamRequest, impersonationTenant *string, clientIP string) (spanResult0 *contracts.DashboardStreamSession, spanResult1 chan contracts.StreamEvent, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tenantScope, err := rules.ResolveStreamScope(principalTenant, req, impersonationTenant)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now().UTC()
	sessionUUID := uuid.New()
	pubID := fmt.Sprintf("dss_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26])

	session := &contracts.DashboardStreamSession{
		ID:                     sessionUUID,
		PublicID:               pubID,
		TenantID:               tenantScope,
		Source:                 "DASHBOARD_GATEWAY",
		ExternalID:             fmt.Sprintf("ext_%d", now.UnixNano()),
		CorrelationID:          sessionUUID,
		PrincipalID:            req.PrincipalID,
		ScopeType:              req.ScopeType,
		ImpersonationTicket:    req.ImpersonationTicket,
		ImpersonationExpiresAt: nil,
		ReauthDeadline:         now.Add(rules.MaxReauthIntervalMinutes * time.Minute),
		LastHeartbeatAt:        now,
		Status:                 contracts.SessionStatusActive,
		CreatedAt:              now,
		UpdatedAt:              now,
		Version:                1,
	}

	if req.ScopeType == contracts.ScopeSupport && req.ImpersonationTicket != nil {
		impExp := now.Add(60 * time.Minute)
		session.ImpersonationExpiresAt = &impExp
	}

	if err := s.repo.CreateStreamSession(ctx, session); err != nil {
		return nil, nil, err
	}

	eventCh := make(chan contracts.StreamEvent, rules.MaxClientQueueDepth)

	s.mu.Lock()
	s.clients[pubID] = &clientStream{
		SessionID:   pubID,
		TenantScope: tenantScope,
		Ch:          eventCh,
	}
	s.mu.Unlock()

	audit := &contracts.DashboardAuditEvent{
		ID:            uuid.New(),
		PublicID:      fmt.Sprintf("dae_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26]),
		TenantID:      tenantScope,
		Source:        "DASHBOARD_GATEWAY",
		ExternalID:    fmt.Sprintf("stm_%d", now.UnixNano()),
		CorrelationID: sessionUUID,
		OperatorID:    req.PrincipalID,
		EventType:     "STREAM_CONNECTED",
		ResourceID:    pubID,
		Details:       json.RawMessage(fmt.Sprintf(`{"scope_type":"%s"}`, req.ScopeType)),
		IPAddress:     clientIP,
		CreatedAt:     now,
		UpdatedAt:     now,
		Version:       1,
	}
	observability.NoteSwallowed(s.repo.RecordAuditEvent(ctx, audit))

	return session, eventCh, nil
}

func (s *DashboardService) UnregisterStreamSession(ctx context.Context, tenantID, sessionID string) {
	defer observability.TraceEnter(nil)()
	s.mu.Lock()
	if client, ok := s.clients[sessionID]; ok {
		close(client.Ch)
		delete(s.clients, sessionID)
	}
	s.mu.Unlock()
	observability.NoteSwallowed(s.repo.UpdateStreamSessionStatus(ctx, tenantID, sessionID, contracts.SessionStatusTerminated))
}

func (s *DashboardService) Heartbeat(ctx context.Context, tenantID, sessionID string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	session, err := s.repo.GetStreamSession(ctx, tenantID, sessionID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	if err := rules.ValidateSessionFreshness(session, now); err != nil {
		observability.NoteSwallowed(s.repo.UpdateStreamSessionStatus(ctx, tenantID, sessionID, contracts.SessionStatusRevoked))
		s.UnregisterStreamSession(ctx, tenantID, sessionID)
		return err
	}

	return s.repo.TouchHeartbeat(ctx, tenantID, sessionID, now)
}

func (s *DashboardService) BroadcastDelta(tenantScope string, event contracts.StreamEvent) {
	defer observability.TraceEnter(nil)()
	s.mu.RLock()
	defer s.mu.RUnlock()

	for sessionID, client := range s.clients {
		if client.TenantScope == tenantScope || client.TenantScope == "PLATFORM_WIDE" || tenantScope == "PLATFORM_WIDE" {
			dropDelta, sendResync, _ := rules.EvaluateBackpressureCapacity(len(client.Ch))
			if dropDelta {
				if sendResync {
					resyncEvent := contracts.StreamEvent{
						ID:           fmt.Sprintf("resync_%d", time.Now().UnixNano()),
						Event:        "RESYNC_REQUIRED",
						Data:         json.RawMessage(`{"reason":"client_buffer_overflow"}`),
						TimestampUTC: time.Now().UTC(),
					}
					select {
					case client.Ch <- resyncEvent:
					default:
					}
				}
				continue
			}

			select {
			case client.Ch <- event:
			default:
				_ = sessionID
			}
		}
	}
}

func (s *DashboardService) PreviewAction(ctx context.Context, principalTenant string, req *contracts.PreviewActionRequest, clientIP string) (spanResult0 *contracts.PreviewActionResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	currentVersion, visualState, err := s.repo.GetSagaStateSnapshot(ctx, principalTenant, req.SagaID)
	if err != nil {
		return nil, err
	}

	if err := rules.ValidateVisualStateActionability(visualState, req.ActionType); err != nil {
		return nil, err
	}

	requiresSecondApprover := false
	moneyAtStake := 0.0000
	affectedSteps := []string{"payment_debit", "inventory_reserve"}
	commands := []string{fmt.Sprintf("EXECUTE_%s", req.ActionType)}
	compensations := []string{}
	customerImpact := "LOW: Transaction remains paused pending verification"

	if req.ActionType == contracts.ActionForceCompensate {
		requiresSecondApprover = true
		moneyAtStake = 450.0000
		compensations = append(compensations, "refund_payment", "release_inventory")
		customerImpact = "HIGH: Order will be cancelled and money refunded"
	}

	now := time.Now().UTC()
	previewUUID := uuid.New()
	pubPreviewID := fmt.Sprintf("dap_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26])
	expiresAt := now.Add(10 * time.Minute)

	previewDetails, _ := json.Marshal(map[string]interface{}{
		"visual_state": visualState,
		"impact":       customerImpact,
	})

	preview := &contracts.DashboardActionPreview{
		ID:                     previewUUID,
		PublicID:               pubPreviewID,
		TenantID:               principalTenant,
		Source:                 "INTERVENTION_ENGINE",
		ExternalID:             fmt.Sprintf("act_%d", now.UnixNano()),
		CorrelationID:          previewUUID,
		SagaID:                 req.SagaID,
		ActionType:             req.ActionType,
		SnapshotVersion:        currentVersion,
		RequiresSecondApprover: requiresSecondApprover,
		MoneyAtStake:           moneyAtStake,
		PreviewPayload:         previewDetails,
		Status:                 contracts.ActionStatusPendingConfirmation,
		ExpiresAt:              expiresAt,
		CreatedAt:              now,
		UpdatedAt:              now,
		Version:                1,
	}

	if err := s.repo.CreateActionPreview(ctx, preview); err != nil {
		return nil, err
	}

	audit := &contracts.DashboardAuditEvent{
		ID:            uuid.New(),
		PublicID:      fmt.Sprintf("dae_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26]),
		TenantID:      principalTenant,
		Source:        "INTERVENTION_ENGINE",
		ExternalID:    fmt.Sprintf("aud_%d", now.UnixNano()),
		CorrelationID: previewUUID,
		OperatorID:    req.OperatorID,
		EventType:     "ACTION_PREVIEWED",
		ResourceID:    pubPreviewID,
		Details:       previewDetails,
		IPAddress:     clientIP,
		CreatedAt:     now,
		UpdatedAt:     now,
		Version:       1,
	}
	observability.NoteSwallowed(s.repo.RecordAuditEvent(ctx, audit))

	return &contracts.PreviewActionResponse{
		PreviewID:              pubPreviewID,
		SagaID:                 req.SagaID,
		ActionType:             req.ActionType,
		SnapshotVersion:        currentVersion,
		RequiresSecondApprover: requiresSecondApprover,
		MoneyAtStake:           moneyAtStake,
		AffectedSteps:          affectedSteps,
		Commands:               commands,
		Compensations:          compensations,
		CustomerImpact:         customerImpact,
		ExpiresAt:              expiresAt,
		Details:                previewDetails,
	}, nil
}

func (s *DashboardService) ExecuteAction(ctx context.Context, principalTenant string, req *contracts.ExecuteActionRequest, clientIP string) (spanResult0 *contracts.ExecuteActionResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	preview, err := s.repo.GetActionPreview(ctx, principalTenant, req.PreviewID)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	if now.After(preview.ExpiresAt) {
		observability.NoteSwallowed(s.repo.UpdateActionPreviewStatus(ctx, principalTenant, req.PreviewID, contracts.ActionStatusExpired))
		return nil, appErrors.NewPreconditionFailedError("action preview has expired; generate a new preview")
	}

	if preview.Status != contracts.ActionStatusPendingConfirmation {
		return nil, appErrors.NewPreconditionFailedError(fmt.Sprintf("action preview status is not pending confirmation: %s", preview.Status))
	}

	if preview.RequiresSecondApprover {
		if req.SecondApproverID == nil || strings.TrimSpace(*req.SecondApproverID) == "" {
			return nil, appErrors.NewForbiddenError("action requires dual approver authorization; second_approver_id is required")
		}
		if *req.SecondApproverID == req.OperatorID {
			return nil, appErrors.NewForbiddenError("second approver cannot be the initiating operator")
		}
	}

	currentVersion, _, err := s.repo.GetSagaStateSnapshot(ctx, principalTenant, preview.SagaID)
	if err != nil {
		return nil, err
	}

	if err := rules.ValidateSnapshotMatch(preview.SnapshotVersion, currentVersion); err != nil {
		observability.NoteSwallowed(s.repo.UpdateActionPreviewStatus(ctx, principalTenant, req.PreviewID, contracts.ActionStatusMismatchRejected))
		return nil, err
	}

	if err := s.repo.UpdateActionPreviewStatus(ctx, principalTenant, req.PreviewID, contracts.ActionStatusConfirmed); err != nil {
		return nil, err
	}

	audit := &contracts.DashboardAuditEvent{
		ID:            uuid.New(),
		PublicID:      fmt.Sprintf("dae_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26]),
		TenantID:      principalTenant,
		Source:        "INTERVENTION_ENGINE",
		ExternalID:    fmt.Sprintf("exe_%d", now.UnixNano()),
		CorrelationID: preview.ID,
		OperatorID:    req.OperatorID,
		EventType:     "ACTION_SUBMITTED",
		ResourceID:    req.InterventionID,
		Details:       json.RawMessage(fmt.Sprintf(`{"action":"%s","saga_id":"%s"}`, preview.ActionType, preview.SagaID)),
		IPAddress:     clientIP,
		CreatedAt:     now,
		UpdatedAt:     now,
		Version:       1,
	}
	observability.NoteSwallowed(s.repo.RecordAuditEvent(ctx, audit))

	return &contracts.ExecuteActionResponse{
		Success:        true,
		InterventionID: req.InterventionID,
		ActionType:     preview.ActionType,
		SagaID:         preview.SagaID,
		ExecutedAt:     now,
	}, nil
}

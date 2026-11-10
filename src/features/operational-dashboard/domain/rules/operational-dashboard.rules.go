package rules

import (
	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"strings"
	"time"
)

const (
	MaxReauthIntervalMinutes = 5
	DefaultHeartbeatSeconds  = 15
	MaxDataAgeSeconds        = 60
	MaxClientQueueDepth      = 50
)

func ResolveStreamScope(principalTenant string, req *contracts.ConnectStreamRequest, impersonationTenant *string) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if req == nil {
		return "", appErrors.NewValidationError("stream request is nil")
	}

	principalTenant = strings.TrimSpace(principalTenant)
	if principalTenant == "" {
		return "", appErrors.NewUnauthorizedError("principal tenant identity is unauthenticated")
	}

	switch req.ScopeType {
	case contracts.ScopeTenantAdmin:
		return principalTenant, nil

	case contracts.ScopeSupport:
		if impersonationTenant == nil || strings.TrimSpace(*impersonationTenant) == "" {
			return "", appErrors.NewForbiddenError("support stream requires active impersonation ticket binding")
		}
		return strings.TrimSpace(*impersonationTenant), nil

	case contracts.ScopeOperator:
		if strings.TrimSpace(req.RequestedTenant) != "" {
			return strings.TrimSpace(req.RequestedTenant), nil
		}
		return "PLATFORM_WIDE", nil

	default:
		return "", appErrors.NewValidationError("unknown stream scope type")
	}
}

func ValidateSessionFreshness(session *contracts.DashboardStreamSession, now time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if session == nil {
		return appErrors.NewNotFoundError("stream session not found")
	}

	if session.Status == contracts.SessionStatusRevoked || session.Status == contracts.SessionStatusTerminated {
		return appErrors.NewForbiddenError(fmt.Sprintf("stream session is in inactive status: %s", session.Status))
	}

	if now.After(session.ReauthDeadline) {
		return appErrors.NewUnauthorizedError("stream session reauthorization deadline expired")
	}

	if session.ImpersonationExpiresAt != nil && now.After(*session.ImpersonationExpiresAt) {
		return appErrors.NewForbiddenError("stream impersonation session expired")
	}

	return nil
}

func SanitizeTileFreshness(tile *contracts.DashboardTileSnapshot, now time.Time) {
	defer observability.TraceEnter(nil)()
	if tile == nil {
		return
	}

	ageMs := now.Sub(tile.SourceTimestamp).Milliseconds()
	if ageMs < 0 {
		ageMs = 0
	}
	tile.DataAgeMs = ageMs

	if ageMs > (MaxDataAgeSeconds * 1000) {
		tile.IsStale = true
	}
}

func ValidateSnapshotMatch(previewSnapshotVersion int64, currentSagaVersion int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if previewSnapshotVersion != currentSagaVersion {
		return appErrors.NewPreconditionFailedError(fmt.Sprintf("state snapshot mismatch: preview bound to version %d, but current saga version is %d", previewSnapshotVersion, currentSagaVersion))
	}
	return nil
}

func ValidateVisualStateActionability(stepState contracts.VisualState, action contracts.ActionType) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if stepState == contracts.VisualStateUnknown && action == contracts.ActionForceCompensate {
		return appErrors.NewPreconditionFailedError("cannot force compensate step in UNKNOWN state without fact-finding resolution")
	}
	return nil
}

func EvaluateBackpressureCapacity(currentQueueLen int) (dropDelta bool, sendResync bool, terminate bool) {
	defer observability.TraceEnter(nil)()
	if currentQueueLen >= MaxClientQueueDepth {
		return true, true, false
	}
	return false, false, false
}

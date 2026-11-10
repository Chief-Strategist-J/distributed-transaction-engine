package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/features/operational-dashboard/domain/rules"
)

func TestResolveStreamScope_TenantAdmin(t *testing.T) {
	req := &contracts.ConnectStreamRequest{
		PrincipalID:     "user-1",
		ScopeType:       contracts.ScopeTenantAdmin,
		RequestedTenant: "tenant-evil",
	}

	scope, err := rules.ResolveStreamScope("tenant-actual", req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope != "tenant-actual" {
		t.Fatalf("expected scope tenant-actual, got %s", scope)
	}
}

func TestResolveStreamScope_SupportWithTicket(t *testing.T) {
	ticketTenant := "tenant-client"
	req := &contracts.ConnectStreamRequest{
		PrincipalID:         "support-rep",
		ScopeType:           contracts.ScopeSupport,
		ImpersonationTicket: &ticketTenant,
	}

	scope, err := rules.ResolveStreamScope("support-org", req, &ticketTenant)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope != "tenant-client" {
		t.Fatalf("expected scope tenant-client, got %s", scope)
	}
}

func TestResolveStreamScope_SupportMissingTicket(t *testing.T) {
	req := &contracts.ConnectStreamRequest{
		PrincipalID: "support-rep",
		ScopeType:   contracts.ScopeSupport,
	}

	_, err := rules.ResolveStreamScope("support-org", req, nil)
	if err == nil {
		t.Fatal("expected error for support scope without impersonation ticket")
	}
}

func TestResolveStreamScope_OperatorPlatformWide(t *testing.T) {
	req := &contracts.ConnectStreamRequest{
		PrincipalID: "sre-lead",
		ScopeType:   contracts.ScopeOperator,
	}

	scope, err := rules.ResolveStreamScope("platform-org", req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope != "PLATFORM_WIDE" {
		t.Fatalf("expected PLATFORM_WIDE, got %s", scope)
	}
}

func TestValidateSessionFreshness_ReauthExpired(t *testing.T) {
	now := time.Now().UTC()
	session := &contracts.DashboardStreamSession{
		Status:         contracts.SessionStatusActive,
		ReauthDeadline: now.Add(-10 * time.Second),
	}

	err := rules.ValidateSessionFreshness(session, now)
	if err == nil {
		t.Fatal("expected reauth deadline expiration error")
	}
}

func TestSanitizeTileFreshness_StaleDetection(t *testing.T) {
	now := time.Now().UTC()
	tile := &contracts.DashboardTileSnapshot{
		SourceTimestamp: now.Add(-120 * time.Second),
		IsStale:         false,
	}

	rules.SanitizeTileFreshness(tile, now)
	if !tile.IsStale {
		t.Fatal("expected tile to be marked stale after 120s")
	}
	if tile.DataAgeMs < 120000 {
		t.Fatalf("expected DataAgeMs >= 120000, got %d", tile.DataAgeMs)
	}
}

func TestValidateSnapshotMatch(t *testing.T) {
	if err := rules.ValidateSnapshotMatch(5, 5); err != nil {
		t.Fatalf("expected match, got error: %v", err)
	}

	if err := rules.ValidateSnapshotMatch(5, 6); err == nil {
		t.Fatal("expected error on snapshot mismatch")
	}
}

func TestValidateVisualStateActionability_UnknownBlock(t *testing.T) {
	err := rules.ValidateVisualStateActionability(contracts.VisualStateUnknown, contracts.ActionForceCompensate)
	if err == nil {
		t.Fatal("expected error preventing force compensation on UNKNOWN step")
	}

	err = rules.ValidateVisualStateActionability(contracts.VisualStateFailed, contracts.ActionForceCompensate)
	if err != nil {
		t.Fatalf("expected success for FAILED step, got: %v", err)
	}
}

func TestEvaluateBackpressureCapacity(t *testing.T) {
	drop, resync, _ := rules.EvaluateBackpressureCapacity(10)
	if drop || resync {
		t.Fatal("expected no drop/resync for small queue")
	}

	drop, resync, _ = rules.EvaluateBackpressureCapacity(55)
	if !drop || !resync {
		t.Fatal("expected drop and resync when queue depth exceeds threshold")
	}
}


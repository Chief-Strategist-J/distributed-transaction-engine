package tests

import (
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	rules "distributed-transaction-engine/src/features/transaction-query-api/domain/rules"
)

func TestDerivePrincipal_TenantUser(t *testing.T) {
	caller := contracts.AuthenticatedCaller{
		PrincipalID: "user-alice",
		TenantID:    "tenant-100",
		Roles:       []string{"operator"},
	}

	principal, err := rules.DerivePrincipal(caller)
	if err != nil {
		t.Fatalf("unexpected error deriving principal: %v", err)
	}

	if principal.TenantID != "tenant-100" {
		t.Fatalf("expected tenant-100, got %s", principal.TenantID)
	}
	if principal.Audience != contracts.AudienceOperator {
		t.Fatalf("expected AudienceOperator, got %s", principal.Audience)
	}
}

func TestBuildInjectedPredicates_RootLevel(t *testing.T) {
	principal := contracts.Principal{
		PrincipalID: "user-1",
		TenantID:    "tenant-alpha",
		Audience:    contracts.AudienceTenantUser,
	}

	injected := rules.BuildInjectedPredicates(principal, "tenant-alpha")
	if injected.TenantID != "tenant-alpha" {
		t.Fatalf("expected injected tenant-alpha, got %s", injected.TenantID)
	}
}

func TestToMinorUnits_CurrencyCalculation(t *testing.T) {
	minor, err := rules.ToMinorUnits("125.50", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if minor != "12550" {
		t.Fatalf("expected 12550, got %s", minor)
	}

	_, err = rules.ToMinorUnits("125.505", 2)
	if err == nil {
		t.Fatal("expected error for decimal precision exceeding currency exponent")
	}
}

func TestEvaluateQueryTenant_ImpersonationExpiry(t *testing.T) {
	now := time.Now().UTC()
	past := now.Add(-10 * time.Minute)

	principal := contracts.Principal{
		PrincipalID: "support-user",
		TenantID:    "tenant-platform",
		Audience:    contracts.AudienceOperator,
		Impersonation: &contracts.ImpersonationSession{
			OperatorID:  "support-user",
			TenantScope: []string{"tenant-target"},
			ExpiresAt:   past,
		},
	}

	target := "tenant-target"
	_, err := rules.EvaluateQueryTenant(principal, &target, now)
	if err == nil {
		t.Fatal("expected error for expired impersonation session")
	}
}

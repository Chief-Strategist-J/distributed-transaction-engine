package tests

import (
	"context"
	"testing"
	"time"

	audittrail "distributed-transaction-engine/src/features/audit-trail"
	auth "distributed-transaction-engine/src/features/authentication-authorization"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/features/authentication-authorization/domain/rules"
)

func TestAuth_LifecycleAndEnforcement(t *testing.T) {
	ctx := context.Background()
	authRepo := auth.NewMemoryRepository()
	auditRepo := audittrail.NewMemoryRepository()
	auditSvc := audittrail.NewDefaultService(auditRepo)
	svc := auth.NewDefaultService(authRepo, auditSvc)

	tenantID := "tenant-alpha"

	apiKeyRes, err := svc.RegisterAPIKey(ctx, contracts.RegisterAPIKeyCommand{
		TenantID: tenantID,
		Name:     "integration-client",
		Scopes:   []string{"tx:start", "tx:view"},
	})
	if err != nil {
		t.Fatalf("failed to register api key: %v", err)
	}

	authCtx, err := svc.AuthenticateAPIKey(ctx, contracts.AuthenticateAPIKeyCommand{
		RawKey:   apiKeyRes.SecretKey,
		TenantID: tenantID,
	})
	if err != nil {
		t.Fatalf("failed to authenticate valid api key: %v", err)
	}
	if authCtx.CallerType != contracts.CallerTypeAPIKey {
		t.Fatalf("expected caller type API_KEY, got %s", authCtx.CallerType)
	}

	err = svc.RevokeCredential(ctx, contracts.RevokeCredentialCommand{
		TenantID:         tenantID,
		TargetType:       contracts.RevocationTargetTypeKey,
		TargetID:         apiKeyRes.PublicID,
		RevocationReason: "security key rotation",
		ExpiresAt:        time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("failed to revoke api key: %v", err)
	}

	_, err = svc.AuthenticateAPIKey(ctx, contracts.AuthenticateAPIKeyCommand{
		RawKey:   apiKeyRes.SecretKey,
		TenantID: tenantID,
	})
	if err == nil {
		t.Fatal("expected authentication error on revoked api key")
	}

	spiffeID := "spiffe://prod.internal/ns/core/sa/order-service"
	_, err = svc.RegisterServiceIdentity(ctx, contracts.RegisterServiceIdentityCommand{
		TenantID:       tenantID,
		SPIFFEID:       spiffeID,
		TrustDomain:    "prod.internal",
		AllowedMethods: []string{"StartSaga", "CancelSaga"},
	})
	if err != nil {
		t.Fatalf("failed to register service identity: %v", err)
	}

	svcAuthCtx, err := svc.AuthenticateService(ctx, contracts.AuthenticateServiceCommand{
		SPIFFEID:    spiffeID,
		TrustDomain: "prod.internal",
		Method:      "StartSaga",
	})
	if err != nil {
		t.Fatalf("failed to authenticate allowed service method: %v", err)
	}
	if svcAuthCtx.SPIFFEID != spiffeID {
		t.Fatalf("expected spiffe ID match, got %s", svcAuthCtx.SPIFFEID)
	}

	_, err = svc.AuthenticateService(ctx, contracts.AuthenticateServiceCommand{
		SPIFFEID:    spiffeID,
		TrustDomain: "prod.internal",
		Method:      "ForceComplete",
	})
	if err == nil {
		t.Fatal("expected forbidden error for unauthorized method ForceComplete")
	}

	providerSecret := []byte("stripe-webhook-secret-99")
	payload := []byte(`{"id":"evt_123","type":"charge.captured"}`)
	now := time.Now().UTC()
	nonce := "nonce_stripe_001"
	sig := rules.ComputeWebhookSignature(providerSecret, payload, now, nonce)

	webhookCmd := contracts.AuthenticateWebhookCommand{
		Provider:       "stripe",
		TenantID:       tenantID,
		RawPayload:     payload,
		Signature:      sig,
		Timestamp:      now,
		Nonce:          nonce,
		ProviderSecret: providerSecret,
	}

	whCtx, err := svc.AuthenticateWebhook(ctx, webhookCmd)
	if err != nil {
		t.Fatalf("failed to authenticate valid webhook: %v", err)
	}
	if whCtx.CallerType != contracts.CallerTypeProviderWebhook {
		t.Fatalf("expected caller type PROVIDER_WEBHOOK, got %s", whCtx.CallerType)
	}

	_, err = svc.AuthenticateWebhook(ctx, webhookCmd)
	if err == nil {
		t.Fatal("expected conflict error on replayed webhook nonce")
	}

	denyCmd := contracts.AuthorizeRequestCommand{
		Auth:             *authCtx,
		ResourceTenantID: "tenant-bravo",
		ResourceID:       "saga-isolated-123",
		Action:           "tx:view",
	}

	authRes, err := svc.Authorize(ctx, denyCmd)
	if err != nil {
		t.Fatalf("unexpected error during authorize: %v", err)
	}
	if authRes.Decision != contracts.AuthDecisionDeny {
		t.Fatalf("expected DENY for cross-tenant access, got %s", authRes.Decision)
	}

	decisions, err := authRepo.GetDecisionsByPrincipal(ctx, tenantID, authCtx.PrincipalID, 10)
	if err != nil {
		t.Fatalf("failed to get decision records: %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("expected 1 audit decision recorded, got %d", len(decisions))
	}
	if decisions[0].Decision != contracts.AuthDecisionDeny {
		t.Fatalf("expected DENY in decision audit record, got %s", decisions[0].Decision)
	}
}


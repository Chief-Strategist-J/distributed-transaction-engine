package tests

import (
	"context"
	"testing"
	"time"

	auth "distributed-transaction-engine/src/features/authentication-authorization"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/infra/observability"
)

func TestAuth_TracingSpans(t *testing.T) {
	ctx := context.Background()
	tracer := observability.NewTracer("auth-test-tracer")
	repo := auth.NewMemoryRepository()
	svc := auth.NewService(auth.ServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	apiKeyRes, err := svc.RegisterAPIKey(ctx, contracts.RegisterAPIKeyCommand{
		TenantID: "tenant-trace",
		Name:     "trace-key",
		Scopes:   []string{"tx:start"},
	})
	if err != nil {
		t.Fatalf("failed register key: %v", err)
	}

	authCtx, err := svc.AuthenticateAPIKey(ctx, contracts.AuthenticateAPIKeyCommand{
		RawKey:   apiKeyRes.SecretKey,
		TenantID: "tenant-trace",
	})
	if err != nil {
		t.Fatalf("failed authenticate key: %v", err)
	}

	_, err = svc.Authorize(ctx, contracts.AuthorizeRequestCommand{
		Auth:             *authCtx,
		ResourceTenantID: "tenant-trace",
		ResourceID:       "res-1",
		Action:           "tx:start",
	})
	if err != nil {
		t.Fatalf("failed authorize: %v", err)
	}

	err = svc.RevokeCredential(ctx, contracts.RevokeCredentialCommand{
		TenantID:         "tenant-trace",
		TargetType:       contracts.RevocationTargetTypeKey,
		TargetID:         apiKeyRes.PublicID,
		RevocationReason: "testing",
		ExpiresAt:        time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("failed revoke: %v", err)
	}
}


package tests

import (
	"context"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/machines"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/rules"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/adapters"
	"distributed-transaction-engine/src/features/fencing-tokens/service"
	"distributed-transaction-engine/src/infra/observability"
)

func TestIntegration_FencingLeaseLifecycleAndBarriers(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryFencingRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewFencingRules(),
		Machine: machines.NewFencingLeaseMachine(),
		Tracer:  observability.NewTracer("test-fencing"),
	})

	tenantID := "default"
	resourceID := "saga_order_999"

	lease1, err := svc.AcquireLease(ctx, contracts.AcquireLeaseInput{
		TenantID:        tenantID,
		ResourceID:      resourceID,
		HolderID:        "worker_alpha",
		LeaseDurationMs: 10000,
	})
	if err != nil {
		t.Fatalf("unexpected error acquiring lease: %v", err)
	}
	if lease1.Epoch != 1 {
		t.Fatalf("expected epoch 1 on initial acquire, got %d", lease1.Epoch)
	}

	_, err = svc.AcquireLease(ctx, contracts.AcquireLeaseInput{
		TenantID:        tenantID,
		ResourceID:      resourceID,
		HolderID:        "worker_beta",
		LeaseDurationMs: 10000,
	})
	if err == nil {
		t.Fatalf("expected error acquiring busy lease by worker_beta")
	}

	renewed, err := svc.RenewLease(ctx, contracts.RenewLeaseInput{
		TenantID:        tenantID,
		ResourceID:      resourceID,
		HolderID:        "worker_alpha",
		Epoch:           1,
		LeaseDurationMs: 15000,
	})
	if err != nil {
		t.Fatalf("unexpected error renewing lease: %v", err)
	}
	if renewed.Epoch != 1 {
		t.Fatalf("expected epoch to remain 1 after renew, got %d", renewed.Epoch)
	}

	d1, err := svc.ValidateAndFence(ctx, contracts.FencedBarrierInput{
		TenantID:   tenantID,
		ResourceID: resourceID,
		Token:      1,
	})
	if err != nil || !d1.Allowed {
		t.Fatalf("expected token 1 to pass barrier, got allowed=%v err=%v", d1.Allowed, err)
	}

	d2, err := svc.ValidateAndFence(ctx, contracts.FencedBarrierInput{
		TenantID:   tenantID,
		ResourceID: resourceID,
		Token:      1,
	})
	if err != nil || !d2.Allowed {
		t.Fatalf("expected second write with same token 1 to pass barrier (FT-03), got allowed=%v err=%v", d2.Allowed, err)
	}

	if err := svc.ReleaseLease(ctx, contracts.ReleaseLeaseInput{
		TenantID:   tenantID,
		ResourceID: resourceID,
		HolderID:   "worker_alpha",
		Epoch:      1,
	}); err != nil {
		t.Fatalf("unexpected error releasing lease: %v", err)
	}

	lease2, err := svc.AcquireLease(ctx, contracts.AcquireLeaseInput{
		TenantID:        tenantID,
		ResourceID:      resourceID,
		HolderID:        "worker_beta",
		LeaseDurationMs: 10000,
	})
	if err != nil {
		t.Fatalf("unexpected error on takeover after release: %v", err)
	}
	if lease2.Epoch != 2 {
		t.Fatalf("expected epoch 2 after takeover, got %d", lease2.Epoch)
	}

	d3, err := svc.ValidateAndFence(ctx, contracts.FencedBarrierInput{
		TenantID:   tenantID,
		ResourceID: resourceID,
		Token:      2,
	})
	if err != nil || !d3.Allowed {
		t.Fatalf("expected token 2 to pass barrier, got allowed=%v err=%v", d3.Allowed, err)
	}

	dZombie, err := svc.ValidateAndFence(ctx, contracts.FencedBarrierInput{
		TenantID:   tenantID,
		ResourceID: resourceID,
		Token:      1,
	})
	if err != nil {
		t.Fatalf("unexpected error during zombie evaluation: %v", err)
	}
	if dZombie.Allowed {
		t.Fatalf("expected zombie write with token 1 to be rejected after token 2 was accepted")
	}
	if dZombie.ViolationType != "STALE_TOKEN" {
		t.Fatalf("expected violationType STALE_TOKEN, got %s", dZombie.ViolationType)
	}

	time.Sleep(10 * time.Millisecond)
	audits, err := svc.GetAudits(ctx, tenantID, resourceID, 10)
	if err != nil {
		t.Fatalf("unexpected error getting audits: %v", err)
	}
	if len(audits) == 0 {
		t.Fatalf("expected at least 1 audit record for zombie write, got 0")
	}
	if audits[0].AttemptedToken != 1 || audits[0].CurrentToken != 2 {
		t.Fatalf("unexpected audit tokens: attempted=%d, current=%d", audits[0].AttemptedToken, audits[0].CurrentToken)
	}
}


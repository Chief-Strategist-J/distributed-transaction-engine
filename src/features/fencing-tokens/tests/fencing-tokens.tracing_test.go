package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/machines"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/rules"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/adapters"
	"distributed-transaction-engine/src/features/fencing-tokens/service"
	"distributed-transaction-engine/src/infra/observability"
)

func TestTracing_SpanCreation(t *testing.T) {
	ctx := context.Background()
	tracer := observability.NewTracer("fencing-trace-test")
	repo := adapters.NewMemoryFencingRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewFencingRules(),
		Machine: machines.NewFencingLeaseMachine(),
		Tracer:  tracer,
	})

	lease, err := svc.AcquireLease(ctx, contracts.AcquireLeaseInput{
		TenantID:        "default",
		ResourceID:      "saga_trace_01",
		HolderID:        "worker_trace",
		LeaseDurationMs: 5000,
	})
	if err != nil || lease == nil {
		t.Fatalf("expected lease acquire with tracing, got err: %v", err)
	}

	decision, err := svc.ValidateAndFence(ctx, contracts.FencedBarrierInput{
		TenantID:   "default",
		ResourceID: "saga_trace_01",
		Token:      lease.Epoch,
	})
	if err != nil || !decision.Allowed {
		t.Fatalf("expected barrier pass with tracing, got err: %v", err)
	}
}


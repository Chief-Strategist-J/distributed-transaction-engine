package tests

import (
	"context"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/machines"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/rules"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/adapters"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/service"
	"distributed-transaction-engine/src/infra/observability"

	"github.com/google/uuid"
)

func TestService_TracingPropagation(t *testing.T) {
	tracer := observability.NewTracer("occ-tracing-test")
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewOCCRules(),
		Machine: machines.NewOCCStrategyMachine(),
		Tracer:  tracer,
	})

	ctx := context.Background()
	ctx, rootSpan := tracer.StartSpan(ctx, "root_occ_test")
	defer rootSpan.End()

	tenantID := "tenant-trace-occ"
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID:           sagaID,
		PublicID:     "sag_trace11111111111111111111",
		TenantID:     tenantID,
		CurrentState: "RUNNING",
		Version:      1,
		OwnerEpoch:   1,
		UpdatedAt:    time.Now().UTC(),
	})

	res, err := svc.MutateWithOCC(ctx, contracts.CASMutationInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		ExpectedVersion: 1,
		ExpectedEpoch:   1,
		TargetState:     "RUNNING",
		WorkerID:        "worker-trace",
		InputID:         "reply-trace",
		InputType:       contracts.InputTypeReply,
		InputPayload:    `{}`,
	}, 2)

	if err != nil {
		t.Fatalf("mutate failed: %v", err)
	}
	if !res.Succeeded {
		t.Fatalf("expected CAS succeeded")
	}

	state, err := svc.DiagnoseAggregate(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("diagnose failed: %v", err)
	}
	if state.Version != 2 {
		t.Errorf("expected version 2, got %d", state.Version)
	}
}


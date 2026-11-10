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

	"github.com/google/uuid"
)

func TestService_CASSuccessAndVersionBump(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewOCCRules(),
		Machine: machines.NewOCCStrategyMachine(),
	})

	tenantID := "tenant-occ-1"
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID:           sagaID,
		PublicID:     "sag_11111111111111111111111111",
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
		WorkerID:        "worker-1",
		InputID:         "reply-step-1",
		InputType:       contracts.InputTypeReply,
		InputPayload:    `{"stepOutcome":"SUCCESS"}`,
	}, 3)

	if err != nil {
		t.Fatalf("expected CAS success, got error: %v", err)
	}
	if !res.Succeeded {
		t.Fatalf("expected CAS Succeeded=true")
	}
	if res.CurrentVersion != 2 {
		t.Errorf("expected version 2 after mutation, got %d", res.CurrentVersion)
	}
}

func TestService_DuplicateInputSkipped(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewOCCRules(),
		Machine: machines.NewOCCStrategyMachine(),
	})

	tenantID := "tenant-occ-2"
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID:           sagaID,
		PublicID:     "sag_22222222222222222222222222",
		TenantID:     tenantID,
		CurrentState: "RUNNING",
		Version:      1,
		OwnerEpoch:   1,
		UpdatedAt:    time.Now().UTC(),
	})

	input := contracts.CASMutationInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		ExpectedVersion: 1,
		ExpectedEpoch:   1,
		TargetState:     "RUNNING",
		WorkerID:        "worker-1",
		InputID:         "reply-step-duplicate",
		InputType:       contracts.InputTypeReply,
		InputPayload:    `{"stepOutcome":"SUCCESS"}`,
	}

	res1, err := svc.MutateWithOCC(ctx, input, 3)
	if err != nil || !res1.Succeeded {
		t.Fatalf("first execution failed: %v", err)
	}

	inputWorkerB := input
	inputWorkerB.WorkerID = "worker-2"
	inputWorkerB.ExpectedVersion = 1

	res2, err := svc.MutateWithOCC(ctx, inputWorkerB, 3)
	if err != nil {
		t.Fatalf("duplicate check returned unexpected error: %v", err)
	}
	if !res2.Succeeded {
		t.Fatalf("duplicate check should return Succeeded=true")
	}

	state, err := svc.DiagnoseAggregate(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("failed to diagnose state: %v", err)
	}
	if state.Version != 2 {
		t.Errorf("duplicate input should not increment version again, expected 2 got %d", state.Version)
	}
}

func TestService_ZombieWorkerRejected(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewOCCRules(),
		Machine: machines.NewOCCStrategyMachine(),
	})

	tenantID := "tenant-occ-3"
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID:           sagaID,
		PublicID:     "sag_33333333333333333333333333",
		TenantID:     tenantID,
		CurrentState: "RUNNING",
		Version:      5,
		OwnerEpoch:   2,
		UpdatedAt:    time.Now().UTC(),
	})

	_, err := svc.MutateWithOCC(ctx, contracts.CASMutationInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		ExpectedVersion: 5,
		ExpectedEpoch:   1,
		TargetState:     "RUNNING",
		WorkerID:        "zombie-worker",
		InputID:         "reply-zombie",
		InputType:       contracts.InputTypeReply,
		InputPayload:    `{}`,
	}, 3)

	if err == nil {
		t.Fatalf("expected error for zombie worker with stale epoch")
	}
}

func TestService_ExhaustionParksPendingInput(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewOCCRules(),
		Machine: machines.NewOCCStrategyMachine(),
	})

	tenantID := "tenant-occ-4"
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID:           sagaID,
		PublicID:     "sag_44444444444444444444444444",
		TenantID:     tenantID,
		CurrentState: "RUNNING",
		Version:      10,
		OwnerEpoch:   1,
		UpdatedAt:    time.Now().UTC(),
	})

	go func() {
		for i := 11; i < 25; i++ {
			time.Sleep(3 * time.Millisecond)
			repo.SeedSaga(contracts.SagaRootState{
				ID:           sagaID,
				PublicID:     "sag_44444444444444444444444444",
				TenantID:     tenantID,
				CurrentState: "RUNNING",
				Version:      int64(i),
				OwnerEpoch:   1,
				UpdatedAt:    time.Now().UTC(),
			})
		}
	}()

	_, err := svc.MutateWithOCC(ctx, contracts.CASMutationInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		ExpectedVersion: 1,
		ExpectedEpoch:   1,
		TargetState:     "RUNNING",
		WorkerID:        "worker-exhaust",
		InputID:         "reply-exhaust",
		InputType:       contracts.InputTypeReply,
		InputPayload:    `{"data":"must_not_drop"}`,
	}, 1)

	if err == nil {
		t.Fatalf("expected error on budget exhaustion")
	}
}

func TestService_ClientEditConflictReturned409(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewOCCRules(),
		Machine: machines.NewOCCStrategyMachine(),
	})

	tenantID := "tenant-occ-5"
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID:           sagaID,
		PublicID:     "sag_55555555555555555555555555",
		TenantID:     tenantID,
		CurrentState: "RUNNING",
		Version:      3,
		OwnerEpoch:   1,
		UpdatedAt:    time.Now().UTC(),
	})

	_, err := svc.ApplyClientEdit(ctx, contracts.ClientEditInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		ExpectedVersion: 2,
		Parameters:      `{"amount":100}`,
		ActorID:         "operator-ui",
	})

	if err == nil {
		t.Fatalf("expected 409 conflict for stale client expected version")
	}
}

func TestService_ContentionTriggersSerialization(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewOCCRules(),
		Machine: machines.NewOCCStrategyMachine(),
	})

	tenantID := "tenant-occ-6"
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID:           sagaID,
		PublicID:     "sag_66666666666666666666666666",
		TenantID:     tenantID,
		CurrentState: "RUNNING",
		Version:      1,
		OwnerEpoch:   1,
		UpdatedAt:    time.Now().UTC(),
	})

	strat1, err := svc.EvaluateStrategy(ctx, tenantID, sagaID)
	if err != nil || strat1 != contracts.StrategyOCC {
		t.Fatalf("expected initial StrategyOCC, got %s, %v", strat1, err)
	}

	for i := 0; i < 3; i++ {
		_ = repo.RecordContention(ctx, contracts.ContentionJournalEntry{
			ID:               uuid.New(),
			PublicID:         "ocj_test",
			TenantID:         tenantID,
			SagaID:           sagaID,
			AttemptedVersion: 1,
			CurrentVersion:   2,
			AttemptedEpoch:   1,
			CurrentEpoch:     1,
			Diagnosis:        contracts.CauseVersionConflict,
			WorkerID:         "worker-test",
			CreatedAt:        time.Now().UTC(),
		})
	}

	strat2, err := svc.EvaluateStrategy(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("evaluate strategy failed: %v", err)
	}
	if strat2 != contracts.StrategySerialized {
		t.Errorf("expected StrategySerialized after 3 contentions, got %s", strat2)
	}
}

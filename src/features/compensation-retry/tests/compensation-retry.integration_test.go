package tests

import (
	"context"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"distributed-transaction-engine/src/features/compensation-retry/domain/machines"
	"distributed-transaction-engine/src/features/compensation-retry/domain/rules"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/adapters"
	"distributed-transaction-engine/src/features/compensation-retry/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

func TestIntegration_EvaluateService(t *testing.T) {
	repo := adapters.NewMemoryCompensationRetryRepositoryAdapter()
	r := rules.NewCompensationRetryRules()
	m := machines.NewCompensationRetryMachine()
	tracer := observability.NewTracer("test-tracer")

	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   r,
		Machine: m,
		Tracer:  tracer,
	})

	ctx := context.Background()
	sagaID := uuid.New().String()
	tenantID := "tenant-test"
	now := time.Now().UTC()
	firstAttempt := now.Format(time.RFC3339)
	deadline := now.Add(2 * time.Hour).Format(time.RFC3339)

	t.Run("transient 503 triggers retry", func(t *testing.T) {
		input := contracts.EvaluateCompensationRetryInput{
			TenantID:         tenantID,
			SagaID:           sagaID,
			StepID:           "step-charge",
			StepType:         "payment.charge",
			DependencyID:     "stripe",
			CompensationKey:  "comp-1",
			AttemptNumber:    1,
			StatusCode:       503,
			ErrorMessage:     "service unavailable",
			FirstAttemptAt:   firstAttempt,
			BusinessDeadline: deadline,
		}

		decision, err := svc.Evaluate(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decision.Action != "RETRY" {
			t.Errorf("expected Action RETRY, got %s", decision.Action)
		}
		if decision.DelayMillis <= 0 {
			t.Errorf("expected positive DelayMillis, got %d", decision.DelayMillis)
		}
	})

	t.Run("timeout triggers verify ambiguous", func(t *testing.T) {
		input := contracts.EvaluateCompensationRetryInput{
			TenantID:         tenantID,
			SagaID:           sagaID,
			StepID:           "step-charge",
			StepType:         "payment.charge",
			DependencyID:     "stripe",
			CompensationKey:  "comp-1",
			AttemptNumber:    2,
			StatusCode:       504,
			ErrorMessage:     "gateway timeout",
			FirstAttemptAt:   firstAttempt,
			BusinessDeadline: deadline,
		}

		decision, err := svc.Evaluate(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decision.Action != "VERIFY_AMBIGUOUS" {
			t.Errorf("expected Action VERIFY_AMBIGUOUS, got %s", decision.Action)
		}
	})

	t.Run("paused step type pauses retry", func(t *testing.T) {
		err := svc.PauseStepType(ctx, tenantID, "payment.charge")
		if err != nil {
			t.Fatalf("unexpected error pausing: %v", err)
		}

		input := contracts.EvaluateCompensationRetryInput{
			TenantID:         tenantID,
			SagaID:           sagaID,
			StepID:           "step-charge",
			StepType:         "payment.charge",
			DependencyID:     "stripe",
			CompensationKey:  "comp-1",
			AttemptNumber:    3,
			StatusCode:       503,
			ErrorMessage:     "service unavailable",
			FirstAttemptAt:   firstAttempt,
			BusinessDeadline: deadline,
		}

		decision, err := svc.Evaluate(ctx, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decision.Action != "PAUSE_STEP_TYPE" {
			t.Errorf("expected Action PAUSE_STEP_TYPE, got %s", decision.Action)
		}
	})
}

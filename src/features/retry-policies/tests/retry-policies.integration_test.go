package tests

import (
	"context"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
	"distributed-transaction-engine/src/features/retry-policies/infrastructure/adapters"
	"distributed-transaction-engine/src/features/retry-policies/service"
	"distributed-transaction-engine/src/infra/observability"

	"github.com/google/uuid"
)

func TestEndToEnd_RetryEvaluationLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryRetryRepositoryAdapter()
	tracer := observability.NewTracer("retry-policies-test")
	svc := service.NewService(service.ServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	now := time.Now().UTC()
	input := contracts.EvaluateRetryInput{
		TenantID:         "tenant_default",
		SagaID:           uuid.New().String(),
		StepID:           "step_reserve_funds",
		AttemptNumber:    1,
		StatusCode:       503,
		ErrorMessage:     "Service Unavailable",
		RetryAfterHeader: "3",
		Capability: contracts.StepCapability{
			IdempotentByKey: true,
			StatusQuery:     true,
			EffectType:      "HOLD",
		},
		Position:         contracts.PositionCompensable,
		DependencyID:     "payments_api",
		FirstAttemptTime: now.Format(time.RFC3339),
		Policy: contracts.RetryPolicyConfig{
			MaxAttempts:        5,
			MaxElapsedSeconds:  300,
			BaseIntervalMillis: 200,
			MaxIntervalMillis:  10000,
			Algorithm:          contracts.AlgorithmFullJitter,
		},
	}

	decision, err := svc.Evaluate(ctx, input)
	if err != nil {
		t.Fatalf("unexpected evaluate error: %v", err)
	}
	if decision.Action != contracts.ActionRetry {
		t.Fatalf("expected ACTION_RETRY, got %s", decision.Action)
	}
	if decision.Delay < 3*time.Second {
		t.Fatalf("expected delay >= 3s due to Retry-After, got %v", decision.Delay)
	}

	for i := 0; i < 5; i++ {
		svc.RecordBreakerFailure(ctx, "payments_api")
	}

	breakerDecision, err := svc.Evaluate(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error on breaker evaluation: %v", err)
	}
	if breakerDecision.Action != contracts.ActionPostponeBreaker {
		t.Fatalf("expected POSTPONE_BREAKER for open circuit breaker, got %s", breakerDecision.Action)
	}
}


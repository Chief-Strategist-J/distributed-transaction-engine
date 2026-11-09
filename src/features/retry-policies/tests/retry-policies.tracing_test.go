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

func TestRetryPoliciesTracingLifecycle(t *testing.T) {
	tracer := observability.NewTracer("retry-tracer-test")
	repo := adapters.NewMemoryRetryRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	ctx, rootSpan := tracer.StartSpan(context.Background(), "POST /api/v1/retry-policies/evaluate")
	defer rootSpan.End()

	now := time.Now().UTC()
	input := contracts.EvaluateRetryInput{
		TenantID:         "tenant_trace",
		SagaID:           uuid.New().String(),
		StepID:           "step_trace_step",
		AttemptNumber:    1,
		StatusCode:       500,
		ErrorMessage:     "Internal Server Error",
		FirstAttemptTime: now.Format(time.RFC3339),
		Capability: contracts.StepCapability{
			IdempotentByKey: true,
		},
		Position: contracts.PositionCompensable,
		Policy: contracts.RetryPolicyConfig{
			MaxAttempts:        3,
			MaxElapsedSeconds:  60,
			BaseIntervalMillis: 200,
			MaxIntervalMillis:  5000,
			Algorithm:          contracts.AlgorithmFullJitter,
		},
	}

	decision, err := svc.Evaluate(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error under active trace: %v", err)
	}
	if decision.Action != contracts.ActionRetry {
		t.Fatalf("expected ActionRetry under active trace, got: %s", decision.Action)
	}

	invalidInput := input
	invalidInput.TenantID = ""
	_, err = svc.Evaluate(ctx, invalidInput)
	if err == nil {
		t.Fatalf("expected validation error and span error recording on invalid tenantId, got nil")
	}
}


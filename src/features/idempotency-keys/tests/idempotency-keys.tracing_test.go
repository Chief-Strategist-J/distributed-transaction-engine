package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/features/idempotency-keys/infrastructure/adapters"
	"distributed-transaction-engine/src/features/idempotency-keys/service"
	"distributed-transaction-engine/src/infra/observability"

	"github.com/google/uuid"
)

func TestIdempotencyTracingLifecycle(t *testing.T) {
	tracer := observability.NewTracer("idempotency-trace-test")
	repo := adapters.NewMemoryIdempotencyRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	ctx, rootSpan := tracer.StartSpan(context.Background(), "POST /api/v1/idempotency/acquire")
	defer rootSpan.End()

	input := contracts.IngestIdempotencyInput{
		ID:             uuid.New().String(),
		PublicID:       "idk_0123456789abcdef0123456789",
		TenantID:       "tenant_test",
		Source:         "API_GATEWAY",
		ExternalID:     "ik_trace_001",
		CorrelationID:  uuid.New().String(),
		PrincipalID:    "user_trace_1",
		OperationID:    "payments.charge",
		IdempotencyKey: "ik_trace_001",
		Payload: map[string]interface{}{
			"chargeId": "chg_001",
		},
	}

	res, err := svc.Ingest(ctx, input)
	if err != nil {
		t.Fatalf("unexpected ingest error under active span: %v", err)
	}
	if res.Outcome != contracts.OutcomeAcquired {
		t.Fatalf("expected OutcomeAcquired, got: %s", res.Outcome)
	}

	tamperedInput := input
	tamperedInput.Payload = map[string]interface{}{
		"chargeId": "chg_tampered_999",
	}
	_, err = svc.Ingest(ctx, tamperedInput)
	if err == nil {
		t.Fatalf("expected conflict error and span error recording on tampered payload, got nil")
	}
}

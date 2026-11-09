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

func TestEndToEnd_IdempotencyLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryIdempotencyRepositoryAdapter()
	tracer := observability.NewTracer("idempotency-test")
	svc := service.NewService(service.ServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	recordID := uuid.New().String()
	corrID := uuid.New().String()
	publicID := "idk_0123456789abcdef0123456789"
	idempotencyKey := "ik_order_checkout_001"
	tenantID := "tenant_acme"
	principalID := "user_alice_101"
	operationID := "orders.checkout"

	input := contracts.IngestIdempotencyInput{
		ID:             recordID,
		PublicID:       publicID,
		TenantID:       tenantID,
		Source:         "API_GATEWAY",
		ExternalID:     idempotencyKey,
		CorrelationID:  corrID,
		PrincipalID:    principalID,
		OperationID:    operationID,
		IdempotencyKey: idempotencyKey,
		Payload: map[string]interface{}{
			"orderId": "ord_1001",
			"amount":  250.00,
		},
	}

	acquireRes, err := svc.Ingest(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error on fresh ingest: %v", err)
	}
	if acquireRes.Outcome != contracts.OutcomeAcquired {
		t.Fatalf("expected ACQUIRED, got %s", acquireRes.Outcome)
	}

	snapshot := contracts.ResponseSnapshot{
		StatusCode: 201,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: map[string]interface{}{
			"orderId": "ord_1001",
			"status":  "CONFIRMED",
		},
	}
	err = svc.CommitSuccess(ctx, tenantID, recordID, snapshot, "ord_1001")
	if err != nil {
		t.Fatalf("unexpected error committing success: %v", err)
	}

	replayInput := input
	replayInput.ID = uuid.New().String()
	replayInput.PublicID = "idk_0123456789abcdef0123456788"

	replayRes, err := svc.Ingest(ctx, replayInput)
	if err != nil {
		t.Fatalf("unexpected error on duplicate ingest: %v", err)
	}
	if replayRes.Outcome != contracts.OutcomeReplay {
		t.Fatalf("expected REPLAY outcome, got %s", replayRes.Outcome)
	}
	if replayRes.Snapshot == nil || replayRes.Snapshot.StatusCode != 201 {
		t.Fatalf("expected replayed snapshot with status 201, got %+v", replayRes.Snapshot)
	}

	tamperedInput := replayInput
	tamperedInput.ID = uuid.New().String()
	tamperedInput.PublicID = "idk_0123456789abcdef0123456787"
	tamperedInput.Payload = map[string]interface{}{
		"orderId": "ord_1001",
		"amount":  9999.00,
	}

	_, err = svc.Ingest(ctx, tamperedInput)
	if err == nil {
		t.Fatalf("expected error on tampered payload, got nil")
	}

	userBobInput := input
	userBobInput.ID = uuid.New().String()
	userBobInput.PublicID = "idk_0123456789abcdef0123456786"
	userBobInput.PrincipalID = "user_bob_202"

	bobRes, err := svc.Ingest(ctx, userBobInput)
	if err != nil {
		t.Fatalf("unexpected error on user bob ingest: %v", err)
	}
	if bobRes.Outcome != contracts.OutcomeAcquired {
		t.Fatalf("expected ACQUIRED for user bob due to multi-principal scope isolation, got %s", bobRes.Outcome)
	}
}


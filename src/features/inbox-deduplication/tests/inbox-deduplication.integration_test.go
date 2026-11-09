package tests

import (
	"context"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	adapters "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/inbox-deduplication/service"
	"distributed-transaction-engine/src/infra/observability"
)

func TestEndToEnd_InboxDeduplicationLifecycle(t *testing.T) {
	repo := adapters.NewMemoryInboxDeduplicationRepositoryAdapter()
	tracer := observability.NewTracer("test")
	svc := service.NewInboxDeduplicationService(service.InboxDeduplicationServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	ctx := context.Background()
	now := time.Now().UTC()

	input := &contracts.IngestMessageInput{
		ID:                 "01931390-1234-7123-8123-123456789abc",
		PublicID:           "inb_01JBM9A1Z00000000000000001",
		TenantID:           "tenant_us_east",
		Source:             "KAFKA_ORDER_TOPIC",
		ExternalID:         "evt_order_completed_9981",
		CorrelationID:      "01931390-1234-7123-8123-123456789abc",
		HandlerName:        "saga_reply_handler",
		HandlerGeneration:  1,
		ProducerEventTime:  now,
		Payload:            map[string]interface{}{"orderId": "ord_100", "status": "CONFIRMED"},
	}

	dec1, err := svc.ProcessIncomingMessage(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error on first ingest: %v", err)
	}
	if dec1.Outcome != contracts.OutcomeApplied {
		t.Fatalf("expected OutcomeApplied, got %v", dec1.Outcome)
	}

	dec2, err := svc.ProcessIncomingMessage(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error on duplicate ingest: %v", err)
	}
	if dec2.Outcome != contracts.OutcomeDuplicate {
		t.Fatalf("expected OutcomeDuplicate, got %v", dec2.Outcome)
	}

	alteredInput := *input
	alteredInput.Payload = map[string]interface{}{"orderId": "ord_100", "status": "TAMPERED"}
	_, err = svc.ProcessIncomingMessage(ctx, &alteredInput)
	if err == nil {
		t.Fatalf("expected error on tampered payload, got nil")
	}

	handlerBInput := *input
	handlerBInput.ID = "01931390-1234-7123-8123-123456789abd"
	handlerBInput.PublicID = "inb_01JBM9A1Z00000000000000002"
	handlerBInput.HandlerName = "loyalty_points_handler"
	dec3, err := svc.ProcessIncomingMessage(ctx, &handlerBInput)
	if err != nil {
		t.Fatalf("unexpected error on second handler ingest: %v", err)
	}
	if dec3.Outcome != contracts.OutcomeApplied {
		t.Fatalf("expected OutcomeApplied for different handler, got %v", dec3.Outcome)
	}
}

func TestInboxValidationFailure(t *testing.T) {
	repo := adapters.NewMemoryInboxDeduplicationRepositoryAdapter()
	tracer := observability.NewTracer("test")
	svc := service.NewInboxDeduplicationService(service.InboxDeduplicationServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	ctx := context.Background()
	invalidInput := &contracts.IngestMessageInput{
		PublicID: "invalid_id",
	}

	_, err := svc.ProcessIncomingMessage(ctx, invalidInput)
	if err == nil {
		t.Fatalf("expected validation error on malformed input, got nil")
	}
}

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

func TestInboxDeduplicationTracingLifecycle(t *testing.T) {
	tracer := observability.NewTracer("inbox-deduplication-trace-test")
	repo := adapters.NewMemoryInboxDeduplicationRepositoryAdapter()
	svc := service.NewInboxDeduplicationService(service.InboxDeduplicationServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	ctx, rootSpan := tracer.StartSpan(context.Background(), "POST /api/v1/inbox/ingest")
	defer rootSpan.End()

	now := time.Now().UTC()
	input := &contracts.IngestMessageInput{
		ID:                "01931390-5555-7123-8123-123456789abc",
		PublicID:          "inb_01JBM9A1Z00000000000000099",
		TenantID:          "tenant_trace_1",
		Source:            "KAFKA_PAYMENT_REPLY",
		ExternalID:        "evt_pay_reply_8821",
		CorrelationID:     "01931390-5555-7123-8123-123456789abc",
		HandlerName:       "payment_reply_worker",
		HandlerGeneration: 1,
		ProducerEventTime: now,
		Payload:           map[string]interface{}{"paymentId": "pay_99", "amount": 2500},
	}

	decision, err := svc.ProcessIncomingMessage(ctx, input)
	if err != nil {
		t.Fatalf("expected successful ingestion under active span, got: %v", err)
	}
	if decision.Outcome != contracts.OutcomeApplied {
		t.Fatalf("expected OutcomeApplied, got: %v", decision.Outcome)
	}

	dupDecision, err := svc.ProcessIncomingMessage(ctx, input)
	if err != nil {
		t.Fatalf("expected safe duplicate under active span, got: %v", err)
	}
	if dupDecision.Outcome != contracts.OutcomeDuplicate {
		t.Fatalf("expected OutcomeDuplicate, got: %v", dupDecision.Outcome)
	}

	tampered := *input
	tampered.Payload = map[string]interface{}{"paymentId": "pay_99", "amount": 99999}
	_, err = svc.ProcessIncomingMessage(ctx, &tampered)
	if err == nil {
		t.Fatalf("expected error and error span recording on tampered payload, got nil")
	}
}

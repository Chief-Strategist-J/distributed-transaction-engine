package tests

import (
	"context"
	"testing"
	"time"

	tracing "distributed-transaction-engine/src/features/distributed-tracing"
	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"github.com/google/uuid"
)

func TestDistributedTracing_LifecycleAndAsyncLinks(t *testing.T) {
	ctx := context.Background()
	repo := tracing.NewMemoryRepository()
	svc := tracing.NewDefaultService(repo)

	tenantID := "tenant-alpha"
	corrID := uuid.New()
	sagaID := uuid.New()

	inboundHeader := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tc, err := svc.StartTrace(ctx, tracing.StartTraceInput{
		TenantID:          tenantID,
		TraceparentHeader: inboundHeader,
		BaggageHeader:     "tenant_tier=gold,deep_mode=true",
		SamplingQuery: contracts.EvaluateSamplingQuery{
			TenantID:      tenantID,
			CorrelationID: corrID,
			IsVIP:         true,
		},
	})
	if err != nil {
		t.Fatalf("failed to start trace: %v", err)
	}
	if !tc.IsSampled || !tc.IsDeepMode {
		t.Fatal("expected trace to be sampled and in deep mode")
	}

	spanCmd := contracts.RecordSpanCommand{
		TenantID:     tenantID,
		TraceID:      tc.TraceID,
		SpanID:       "00f067aa0ba902b7",
		SpanName:     "HTTP POST /api/v1/orders",
		SpanKind:     contracts.SpanKindServer,
		StartTime:    time.Now().UTC().Add(-100 * time.Millisecond),
		EndTime:      time.Now().UTC(),
		StatusCode:   contracts.SpanStatusOK,
		Attributes:   map[string]interface{}{"http.status_code": 200},
		Events:       []contracts.SpanEvent{{Name: "idempotency_cache_hit", Timestamp: time.Now().UTC()}},
	}
	spanRec, err := svc.RecordSpan(ctx, spanCmd)
	if err != nil {
		t.Fatalf("failed to record server span: %v", err)
	}
	if spanRec.SpanName != spanCmd.SpanName {
		t.Fatalf("expected span name match, got %s", spanRec.SpanName)
	}

	linkCmd := contracts.RegisterAsyncLinkCommand{
		TenantID:    tenantID,
		SagaID:      sagaID,
		StepName:    "PaymentStep",
		Traceparent: inboundHeader,
		LinkType:    contracts.AsyncLinkDispatch,
	}
	_, err = svc.RegisterAsyncLink(ctx, linkCmd)
	if err != nil {
		t.Fatalf("failed to register async link: %v", err)
	}

	resolvedLink, err := svc.ResolveAsyncLink(ctx, contracts.ResolveAsyncLinkQuery{
		TenantID: tenantID,
		SagaID:   sagaID,
		StepName: "PaymentStep",
		LinkType: contracts.AsyncLinkDispatch,
	})
	if err != nil {
		t.Fatalf("failed to resolve async link: %v", err)
	}
	if resolvedLink.TraceID != tc.TraceID {
		t.Fatalf("expected resolved link trace ID %s, got %s", tc.TraceID, resolvedLink.TraceID)
	}

	deepCmd := contracts.ActivateDeepSessionCommand{
		TenantID:     tenantID,
		TargetType:   contracts.DeepTargetSagaID,
		TargetValue:  sagaID.String(),
		SamplingRate: 1.0,
		MaxSpans:     1000,
		DurationSec:  600,
	}
	deepRec, err := svc.ActivateDeepSession(ctx, deepCmd)
	if err != nil {
		t.Fatalf("failed to activate deep session: %v", err)
	}
	if deepRec.Status != contracts.DeepSessionActive {
		t.Fatalf("expected deep session status ACTIVE, got %s", deepRec.Status)
	}

	spans, err := svc.GetSpansByTraceID(ctx, tenantID, tc.TraceID)
	if err != nil {
		t.Fatalf("failed to list spans: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(spans))
	}
}


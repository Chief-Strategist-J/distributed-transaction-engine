package tests

import (
	"context"
	"testing"
	"time"

	tracing "distributed-transaction-engine/src/features/distributed-tracing"
	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

func TestDistributedTracing_TracingSpans(t *testing.T) {
	ctx := context.Background()
	tracer := observability.NewTracer("dt-test-tracer")
	repo := tracing.NewMemoryRepository()
	svc := tracing.NewService(tracing.ServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	tenantID := "tenant-test"
	corrID := uuid.New()

	tc, err := svc.StartTrace(ctx, tracing.StartTraceInput{
		TenantID:          tenantID,
		TraceparentHeader: "",
		BaggageHeader:     "",
		SamplingQuery: contracts.EvaluateSamplingQuery{
			TenantID:      tenantID,
			CorrelationID: corrID,
			IsVIP:         false,
		},
	})
	if err != nil {
		t.Fatalf("failed start trace: %v", err)
	}

	_, err = svc.RecordSpan(ctx, contracts.RecordSpanCommand{
		TenantID:   tenantID,
		TraceID:    tc.TraceID,
		SpanID:     tc.SpanID,
		SpanName:   "unit-test-span",
		SpanKind:   contracts.SpanKindInternal,
		StartTime:  time.Now().UTC(),
		EndTime:    time.Now().UTC(),
		StatusCode: contracts.SpanStatusOK,
	})
	if err != nil {
		t.Fatalf("failed record span: %v", err)
	}
}


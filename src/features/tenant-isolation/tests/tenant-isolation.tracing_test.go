package tests

import (
	"context"
	"testing"

	tenantisolation "distributed-transaction-engine/src/features/tenant-isolation"
	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestServicesEmitSpansPreservingTraceAndRecordErrors(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(sdktrace.NewTracerProvider())

	memory := tenantisolation.NewMemoryAdapter()
	runtime := tenantisolation.NewSystemRuntimeAdapter()
	services := tenantisolation.NewServices(tenantisolation.ServicesDeps{
		Registry: memory, Quota: memory, Quarantine: memory, Security: memory, Clock: runtime, IDs: runtime,
		Tracer: observability.NewTracer("tenant-isolation-tracing-test"),
	})
	rootContext, rootSpan := provider.Tracer("root").Start(context.Background(), "root")
	defer rootSpan.End()

	if _, err := services.Registry.CreateTenant(rootContext, contracts.CreateTenantInput{TenantID: "tenant-a", MaxConcurrentSagas: 3, CorrelationID: uuid.New()}); err != nil {
		t.Fatal(err)
	}
	if _, err := services.Quota.CreateSagaQuotaReservation(rootContext, "tenant-ghost"); err == nil {
		t.Fatal("expected an error for an unknown tenant")
	}

	spans := recorder.Ended()
	if len(spans) < 2 {
		t.Fatalf("expected at least two ended spans, got %d", len(spans))
	}
	for _, span := range spans {
		if span.SpanContext().TraceID() != rootSpan.SpanContext().TraceID() {
			t.Fatalf("span %s dropped the trace context", span.Name())
		}
	}
	failed := spans[len(spans)-1]
	if failed.Name() != "tenant_isolation.create_saga_quota_reservation" || failed.Status().Code != codes.Error {
		t.Fatalf("expected the failing reservation span to be marked as error, got %s %v", failed.Name(), failed.Status().Code)
	}
}

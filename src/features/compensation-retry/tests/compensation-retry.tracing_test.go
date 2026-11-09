package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/infra/observability"
)

func TestTracing_SpanCreation(t *testing.T) {
	tracer := observability.NewTracer("compensation-retry-test")
	ctx := context.Background()

	ctx, span := tracer.StartSpan(ctx, "compensation_retry.evaluate")
	if span == nil {
		t.Fatalf("expected non-nil span")
	}
	defer span.End()

	if ctx == nil {
		t.Fatalf("expected non-nil context")
	}
}


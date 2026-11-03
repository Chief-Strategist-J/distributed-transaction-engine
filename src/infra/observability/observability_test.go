package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

func TestLokiLoggerOutput(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := observability.NewLokiLogger(buf, "test-service")

	ctx := context.Background()
	ctx = observability.WithTenantID(ctx, "tenant_corp_1")

	logger.Info(ctx, "workflow validation initiated", observability.String("workflow_name", "order-flow"))

	var entry observability.LokiLogEntry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse loki log entry: %v", err)
	}

	if entry.Level != "INFO" {
		t.Errorf("expected level INFO, got %s", entry.Level)
	}
	if entry.Service != "test-service" {
		t.Errorf("expected service test-service, got %s", entry.Service)
	}
	if entry.TenantID != "tenant_corp_1" {
		t.Errorf("expected tenant_corp_1, got %s", entry.TenantID)
	}
	if entry.Message != "workflow validation initiated" {
		t.Errorf("unexpected message: %s", entry.Message)
	}
	if entry.Attributes["workflow_name"] != "order-flow" {
		t.Errorf("expected workflow_name attribute, got %v", entry.Attributes["workflow_name"])
	}
}

func TestTracerAndErrorRecording(t *testing.T) {
	tracer := observability.NewTracer("test-tracer")
	ctx, span := tracer.StartSpan(context.Background(), "test-span")
	defer span.End()

	appErr := errors.NewValidationError("invalid step payload")
	observability.RecordError(span, appErr)

	if span == nil {
		t.Fatal("expected non-nil span")
	}
	_ = ctx
}

func TestObservabilityMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	wrapped := observability.ObservabilityMiddleware("test-service", handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t_alpha/workflows", nil)
	rec := httptest.NewRecorder()

	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("X-Trace-Id") == "" {
		t.Errorf("expected non-empty X-Trace-Id header")
	}
	if rec.Header().Get("X-Span-Id") == "" {
		t.Errorf("expected non-empty X-Span-Id header")
	}
}


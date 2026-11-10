package tests

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	queryapi "distributed-transaction-engine/src/features/transaction-query-api"
	"distributed-transaction-engine/src/shared/constants"
)

func TestTransactionQueryAPI_TracingContextPropagation(t *testing.T) {
	memRepo := queryapi.NewMemoryAdapter()
	svc := queryapi.NewDefaultService(memRepo)
	h := handlers.NewTransactionQueryHandler(svc)

	traceID := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req := httptest.NewRequest(http.MethodPost, "/api/v1/transactions:search", strings.NewReader(`{"limit": 5}`))
	req.Header.Set(constants.HeaderTraceparent, traceID)
	req.Header.Set(constants.HeaderXTenantID, "tenant-trace-test")
	req.Header.Set(constants.HeaderXRequestID, "req-tq-12345")
	req.Header.Set(constants.HeaderXCorrelationID, "c0a80101-0000-0000-0000-000000000002")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	h.SearchTransactions(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	if gotTp := rec.Header().Get(constants.HeaderTraceparent); gotTp != traceID {
		t.Fatalf("expected traceparent header %s, got %s", traceID, gotTp)
	}

	if gotTid := rec.Header().Get(constants.HeaderXTenantID); gotTid != "tenant-trace-test" {
		t.Fatalf("expected tenant header tenant-trace-test, got %s", gotTid)
	}

	if !strings.Contains(rec.Body.String(), `"success":true`) {
		t.Fatalf("expected successful API response, got %s", rec.Body.String())
	}
}

func TestTransactionQueryAPI_AggregateEndpoint(t *testing.T) {
	memRepo := queryapi.NewMemoryAdapter()
	svc := queryapi.NewDefaultService(memRepo)
	h := handlers.NewTransactionQueryHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/transactions:aggregate", strings.NewReader(`{}`))
	req.Header.Set(constants.HeaderXTenantID, "tenant-test")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	h.AggregateTransactions(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	if !strings.Contains(rec.Body.String(), `"success":true`) {
		t.Fatalf("expected success true in response, got %s", rec.Body.String())
	}
}


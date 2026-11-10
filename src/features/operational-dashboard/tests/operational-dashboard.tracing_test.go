package tests

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	dashboard "distributed-transaction-engine/src/features/operational-dashboard"
	"distributed-transaction-engine/src/features/operational-dashboard/infrastructure/adapters"
	"distributed-transaction-engine/src/shared/constants"
)

func TestOperationalDashboard_TracingContextPropagation(t *testing.T) {
	repo := adapters.NewDashboardMemoryRepositoryAdapter()
	svc := dashboard.NewService(dashboard.DashboardServiceDeps{Repo: repo})
	h := handlers.NewOperationalDashboardHandler(svc)

	traceID := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/overview", nil)
	req.Header.Set(constants.HeaderTraceparent, traceID)
	req.Header.Set(constants.HeaderXTenantID, "tenant-trace-test")
	req.Header.Set(constants.HeaderXRequestID, "req-12345")
	req.Header.Set(constants.HeaderXCorrelationID, "c0a80101-0000-0000-0000-000000000001")

	rec := httptest.NewRecorder()
	h.GetOverview(rec, req)

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

func TestOperationalDashboard_PreviewActionHandler(t *testing.T) {
	repo := adapters.NewDashboardMemoryRepositoryAdapter()
	svc := dashboard.NewService(dashboard.DashboardServiceDeps{Repo: repo})
	h := handlers.NewOperationalDashboardHandler(svc)

	body := `{"saga_id":"saga-preview-1","action_type":"PAUSE","current_saga_version":1,"operator_id":"op-test"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/dashboard/actions/preview", strings.NewReader(body))
	req.Header.Set(constants.HeaderXTenantID, "tenant-test")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	h.PreviewAction(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	if !strings.Contains(rec.Body.String(), "preview_id") {
		t.Fatalf("expected preview_id in response, got %s", rec.Body.String())
	}
}

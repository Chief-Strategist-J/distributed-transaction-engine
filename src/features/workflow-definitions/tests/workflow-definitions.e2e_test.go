package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	"distributed-transaction-engine/src/api/rest/v1/routers"
	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
)

func TestEndToEnd_WorkflowDefinitionsHTTPProtocolLifecycle(t *testing.T) {
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)
	handler := handlers.NewWorkflowDefinitionsHandler(svc)
	router := routers.NewWorkflowDefinitionsRouter(handler)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux)

	tenantID := "tenant_e2e_01"
	correlationID := "c2000000-0000-0000-0000-000000000001"

	bodyPayload := contracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "checkout-saga-dag",
		Version:       1,
		Source:        "E2E_HTTP",
		ExternalID:    "ext_e2e_http_001",
		CorrelationID: correlationID,
		Steps: []contracts.StepDefinition{
			{
				StepID:               "reserve_seat",
				StepName:             "Reserve Flight Seat",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://flights/reserve",
				CompensationAction:   "CANCEL_SEAT",
				CompensationEndpoint: "http://flights/cancel",
				TimeoutSeconds:       15,
				MaxRetries:           2,
				Dependencies:         []string{},
			},
			{
				StepID:               "book_hotel",
				StepName:             "Book Hotel Room",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://hotels/book",
				CompensationAction:   "CANCEL_HOTEL",
				CompensationEndpoint: "http://hotels/cancel",
				TimeoutSeconds:       15,
				MaxRetries:           2,
				Dependencies:         []string{},
			},
			{
				StepID:               "charge_customer",
				StepName:             "Charge Customer Payment",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://billing/charge",
				TimeoutSeconds:       30,
				MaxRetries:           1,
				Dependencies:         []string{"reserve_seat", "book_hotel"},
			},
		},
	}

	payloadBytes, err := json.Marshal(bodyPayload)
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-definitions", bytes.NewReader(payloadBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-tenant-id", tenantID)
	req.Header.Set("x-correlation-id", correlationID)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected HTTP 201 Created, got %d: %s", w.Code, w.Body.String())
	}

	var createResp contracts.WorkflowDefinitionResponse
	if err := json.Unmarshal(w.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !createResp.Success || createResp.Data == nil {
		t.Fatalf("create failed in envelope: %+v", createResp)
	}
	createdID := createResp.Data.ID

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/workflow-definitions/"+createdID, nil)
	getReq.Header.Set("x-tenant-id", tenantID)

	getRecorder := httptest.NewRecorder()
	mux.ServeHTTP(getRecorder, getReq)

	if getRecorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK on GET, got %d: %s", getRecorder.Code, getRecorder.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/workflow-definitions?limit=10", nil)
	listReq.Header.Set("x-tenant-id", tenantID)

	listRecorder := httptest.NewRecorder()
	mux.ServeHTTP(listRecorder, listReq)

	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK on List, got %d: %s", listRecorder.Code, listRecorder.Body.String())
	}
}

func TestEndToEnd_HTTPRejectionOnCyclicPayload(t *testing.T) {
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)
	handler := handlers.NewWorkflowDefinitionsHandler(svc)
	router := routers.NewWorkflowDefinitionsRouter(handler)

	mux := http.NewServeMux()
	router.RegisterRoutes(mux)

	cyclicPayload := contracts.CreateWorkflowDefinitionInput{
		TenantID:      "tenant_cycle_http",
		Name:          "cyclic-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_cycle",
		CorrelationID: "c0000000-0000-0000-0000-000000000001",
		Steps: []contracts.StepDefinition{
			{
				StepID:               "step_alpha",
				StepName:             "Step A",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://a",
				CompensationAction:   "UNDO_A",
				CompensationEndpoint: "http://a/undo",
				TimeoutSeconds:       10,
				Dependencies:         []string{"step_beta"},
			},
			{
				StepID:               "step_beta",
				StepName:             "Step B",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://b",
				CompensationAction:   "UNDO_B",
				CompensationEndpoint: "http://b/undo",
				TimeoutSeconds:       10,
				Dependencies:         []string{"step_alpha"},
			},
		},
	}

	payloadBytes, _ := json.Marshal(cyclicPayload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-definitions", bytes.NewReader(payloadBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected HTTP 409 Conflict for cyclic DAG, got %d: %s", w.Code, w.Body.String())
	}
}


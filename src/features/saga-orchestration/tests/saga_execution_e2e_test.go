package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	"distributed-transaction-engine/src/api/rest/v1/routers"
	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	workflowAdapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowService "distributed-transaction-engine/src/features/workflow-definitions/service"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/types"
)

func TestEndToEnd_SagaHTTPExecutionLifecycle(t *testing.T) {
	tenantID := "tenant_http_e2e_01"
	correlationID := "c5000000-0000-0000-0000-000000000001"

	wfRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfSvc := workflowService.NewWorkflowDefinitionsService(wfRepo)
	wfHandler := handlers.NewWorkflowDefinitionsHandler(wfSvc)
	wfRouter := routers.NewWorkflowDefinitionsRouter(wfHandler)

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{}
	sagaSvc := sagaService.NewSagaOrchestrationService(sagaRepo, wfRepo, dispatcher)
	sagaHandler := handlers.NewSagaHandler(sagaSvc)
	sagaRouter := routers.NewSagaRouter(sagaHandler)

	mux := http.NewServeMux()
	wfRouter.RegisterRoutes(mux)
	sagaRouter.RegisterRoutes(mux)
	wrappedMux := observability.ObservabilityMiddleware("distributed-transaction-engine", mux)

	wfPayload := workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "payment-pipeline",
		Version:       1,
		Source:        "E2E_HTTP",
		ExternalID:    "ext_wf_http_001",
		CorrelationID: correlationID,
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "validate_balance",
				StepName:             "Validate Balance",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://ledger/validate",
				CompensationAction:   "INVALIDATE",
				CompensationEndpoint: "http://ledger/invalidate",
				TimeoutSeconds:       10,
				Dependencies:         []string{},
			},
			{
				StepID:               "capture_payment",
				StepName:             "Capture Payment",
				IsPivot:              true,
				ActionType:           "REST",
				ActionEndpoint:       "http://processor/capture",
				TimeoutSeconds:       15,
				Dependencies:         []string{"validate_balance"},
			},
		},
	}

	wfBytes, _ := json.Marshal(wfPayload)
	wfReq := httptest.NewRequest(http.MethodPost, "/api/v1/workflow-definitions", bytes.NewReader(wfBytes))
	wfReq.Header.Set("Content-Type", "application/json")
	wfReq.Header.Set("x-tenant-id", tenantID)

	wfRec := httptest.NewRecorder()
	wrappedMux.ServeHTTP(wfRec, wfReq)

	if wfRec.Code != http.StatusCreated {
		t.Fatalf("workflow registration failed, expected 201 got %d: %s", wfRec.Code, wfRec.Body.String())
	}

	execPayload := sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "payment-pipeline",
		WorkflowVersion: 1,
		Source:          "HTTP_E2E",
		ExternalID:      "ext_saga_001",
		CorrelationID:   correlationID,
		InitialPayload:  map[string]interface{}{"amount": 100},
	}
	execBytes, _ := json.Marshal(execPayload)
	execReq := httptest.NewRequest(http.MethodPost, "/api/v1/sagas/execute", bytes.NewReader(execBytes))
	execReq.Header.Set("Content-Type", "application/json")
	execReq.Header.Set("x-tenant-id", tenantID)
	execReq.Header.Set("x-correlation-id", correlationID)
	execReq.Header.Set("Idempotency-Key", "idemp_saga_http_001")

	execRec := httptest.NewRecorder()
	wrappedMux.ServeHTTP(execRec, execReq)

	if execRec.Code != http.StatusCreated {
		t.Fatalf("saga execution failed, expected 201 got %d: %s", execRec.Code, execRec.Body.String())
	}
	if execRec.Header().Get("X-Trace-Id") == "" {
		t.Fatalf("expected X-Trace-Id header in saga execution response")
	}

	var execResp types.ApiResponse
	if err := json.Unmarshal(execRec.Body.Bytes(), &execResp); err != nil {
		t.Fatalf("failed to parse execute response: %v", err)
	}

	dataMap, ok := execResp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map in data response, got %T", execResp.Data)
	}
	sagaPublicID, ok := dataMap["publicId"].(string)
	if !ok || sagaPublicID == "" {
		t.Fatalf("expected non-empty publicId in data")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+sagaPublicID, nil)
	getReq.Header.Set("x-tenant-id", tenantID)

	getRec := httptest.NewRecorder()
	wrappedMux.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("failed to retrieve saga, expected 200 got %d: %s", getRec.Code, getRec.Body.String())
	}
}


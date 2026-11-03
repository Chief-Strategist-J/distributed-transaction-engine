package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	service "distributed-transaction-engine/src/features/workflow-definitions/service"
	"distributed-transaction-engine/src/shared/errors"
)

type WorkflowDefinitionsHandler struct {
	svc *service.WorkflowDefinitionsService
}

func NewWorkflowDefinitionsHandler(svc *service.WorkflowDefinitionsService) *WorkflowDefinitionsHandler {
	return &WorkflowDefinitionsHandler{svc: svc}
}

func (h *WorkflowDefinitionsHandler) RegisterWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	var input contracts.CreateWorkflowDefinitionInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_JSON", "Malformed request payload")
		return
	}

	tenantID := r.Header.Get("x-tenant-id")
	if tenantID != "" {
		input.TenantID = tenantID
	}
	correlationID := r.Header.Get("x-correlation-id")
	if correlationID != "" {
		input.CorrelationID = correlationID
	}

	def, err := h.svc.RegisterWorkflow(r.Context(), input)
	if err != nil {
		h.mapAndWriteError(w, err)
		return
	}

	resp := contracts.WorkflowDefinitionResponse{
		Success:    true,
		StatusCode: http.StatusCreated,
		Data:       def,
		Meta: map[string]interface{}{
			"tenantId":      def.TenantID,
			"correlationId": def.CorrelationID,
			"specHash":      def.SpecHash,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *WorkflowDefinitionsHandler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		h.writeError(w, http.StatusBadRequest, "INVALID_PATH", "Missing workflow id in path")
		return
	}
	id := parts[3]

	tenantID := r.Header.Get("x-tenant-id")
	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenantId")
	}

	def, err := h.svc.GetWorkflow(r.Context(), tenantID, id)
	if err != nil {
		h.mapAndWriteError(w, err)
		return
	}

	resp := contracts.WorkflowDefinitionResponse{
		Success:    true,
		StatusCode: http.StatusOK,
		Data:       def,
		Meta: map[string]interface{}{
			"tenantId": def.TenantID,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *WorkflowDefinitionsHandler) ListWorkflows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	tenantID := r.Header.Get("x-tenant-id")
	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenantId")
	}

	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil {
			offset = parsed
		}
	}

	defs, err := h.svc.ListWorkflows(r.Context(), tenantID, limit, offset)
	if err != nil {
		h.mapAndWriteError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"statusCode": http.StatusOK,
		"data":       defs,
		"meta": map[string]interface{}{
			"count": len(defs),
			"limit": limit,
			"offset": offset,
		},
	})
}

func (h *WorkflowDefinitionsHandler) mapAndWriteError(w http.ResponseWriter, err error) {
	if appErr, ok := err.(*errors.ApplicationError); ok {
		h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message)
		return
	}
	h.writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
}

func (h *WorkflowDefinitionsHandler) writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(contracts.WorkflowDefinitionResponse{
		Success:    false,
		StatusCode: status,
		Error: &contracts.ContractsError{
			Code:      code,
			Message:   msg,
			Retryable: status >= 500,
		},
		Meta: map[string]interface{}{
			"errorCode": code,
			"message":   msg,
		},
	})
}


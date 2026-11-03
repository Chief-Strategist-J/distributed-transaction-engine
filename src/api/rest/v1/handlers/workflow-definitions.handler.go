package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	service "distributed-transaction-engine/src/features/workflow-definitions/service"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type WorkflowDefinitionsHandler struct {
	svc    *service.WorkflowDefinitionsService
	tracer *observability.DefaultTracer
	logger *observability.LokiLogger
}

func NewWorkflowDefinitionsHandler(svc *service.WorkflowDefinitionsService) *WorkflowDefinitionsHandler {
	return &WorkflowDefinitionsHandler{
		svc:    svc,
		tracer: observability.NewTracer("workflow-definitions-handler"),
		logger: observability.GetLogger(),
	}
}

func (h *WorkflowDefinitionsHandler) RegisterWorkflow(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "workflow_definitions.register")
	defer span.End()

	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	}

	var input contracts.CreateWorkflowDefinitionInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.logger.Warn(ctx, "failed to decode workflow registration payload",
			observability.String("error", err.Error()),
		)
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

	h.logger.Info(ctx, "initiating workflow registration",
		observability.String("workflow_name", input.Name),
		observability.Int64("version", input.Version),
		observability.String("tenant_id", input.TenantID),
		observability.String("idempotency_key", idempotencyKey),
	)

	def, err := h.svc.RegisterWorkflow(ctx, input)
	if err != nil {
		observability.RecordError(span, err)
		h.logger.Error(ctx, "workflow registration rejected",
			observability.String("workflow_name", input.Name),
			observability.String("error", err.Error()),
			observability.String("tenant_id", input.TenantID),
		)
		h.mapAndWriteError(w, err)
		return
	}

	h.logger.Info(ctx, "workflow successfully registered",
		observability.String("workflow_id", def.PublicID),
		observability.String("spec_hash", def.SpecHash),
		observability.String("tenant_id", def.TenantID),
	)

	resp := contracts.WorkflowDefinitionResponse{
		Success:    true,
		StatusCode: http.StatusCreated,
		Data:       def,
		Meta: map[string]interface{}{
			"tenantId":       def.TenantID,
			"correlationId":  def.CorrelationID,
			"specHash":       def.SpecHash,
			"idempotencyKey": idempotencyKey,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *WorkflowDefinitionsHandler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "workflow_definitions.get")
	defer span.End()

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

	def, err := h.svc.GetWorkflow(ctx, tenantID, id)
	if err != nil {
		observability.RecordError(span, err)
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
	ctx, span := h.tracer.StartSpan(r.Context(), "workflow_definitions.list")
	defer span.End()

	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var tenantID string
	if len(parts) >= 3 && parts[1] == "tenants" {
		tenantID = parts[2]
	}
	if tenantID == "" {
		tenantID = r.Header.Get("x-tenant-id")
	}

	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsedLimit, err := strconv.Atoi(l); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsedOffset, err := strconv.Atoi(o); err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

	defs, err := h.svc.ListWorkflows(ctx, tenantID, limit, offset)
	if err != nil {
		observability.RecordError(span, err)
		h.mapAndWriteError(w, err)
		return
	}

	resp := map[string]interface{}{
		"success":    true,
		"statusCode": http.StatusOK,
		"data":       defs,
		"meta": map[string]interface{}{
			"tenantId": tenantID,
			"limit":    limit,
			"offset":   offset,
			"count":    len(defs),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *WorkflowDefinitionsHandler) mapAndWriteError(w http.ResponseWriter, err error) {
	if appErr, ok := err.(*errors.ApplicationError); ok {
		h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message)
		return
	}

	h.writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal system error occurred")
}

func (h *WorkflowDefinitionsHandler) writeError(w http.ResponseWriter, statusCode int, code string, msg string) {
	resp := contracts.WorkflowDefinitionResponse{
		Success:    false,
		StatusCode: statusCode,
		Error: &contracts.ContractsError{
			Code:      code,
			Message:   msg,
			Retryable: false,
		},
		Meta: map[string]interface{}{},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(resp)
}

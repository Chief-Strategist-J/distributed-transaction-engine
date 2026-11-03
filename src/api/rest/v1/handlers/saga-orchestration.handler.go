package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type SagaHandler struct {
	service *sagaService.SagaOrchestrationService
	tracer  *observability.DefaultTracer
	logger  *observability.LokiLogger
}

func NewSagaHandler(s *sagaService.SagaOrchestrationService) *SagaHandler {
	return &SagaHandler{
		service: s,
		tracer:  observability.NewTracer("saga-handler"),
		logger:  observability.GetLogger(),
	}
}

func (h *SagaHandler) ExecuteSaga(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "saga.execute")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	traceparent := r.Header.Get(constants.HeaderTraceparent)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)
	idempKey := r.Header.Get("Idempotency-Key")

	var input sagaContracts.ExecuteSagaInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.logger.Warn(ctx, "failed to decode execute saga request",
			observability.String("error", err.Error()),
		)
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID)
		return
	}

	if tenantID != "" {
		input.TenantID = tenantID
	}
	if corrID != "" {
		input.CorrelationID = corrID
	}

	h.logger.Info(ctx, "executing saga workflow",
		observability.String("workflow_name", input.WorkflowName),
		observability.Int64("workflow_version", input.WorkflowVersion),
		observability.String("tenant_id", input.TenantID),
		observability.String("idempotency_key", idempKey),
	)

	result, err := h.service.StartSaga(ctx, input)
	if err != nil {
		observability.RecordError(span, err)
		h.logger.Error(ctx, "saga start failed",
			observability.String("workflow_name", input.WorkflowName),
			observability.String("error", err.Error()),
		)

		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID)
		return
	}

	statusCode := http.StatusAccepted
	if result.CurrentState != sagaContracts.SagaStateCreated {
		statusCode = http.StatusOK
	}

	w.Header().Set("Location", "/api/v1/sagas/"+result.PublicID)
	w.WriteHeader(statusCode)
	if traceparent != "" {
		w.Header().Set(constants.HeaderTraceparent, traceparent)
	}

	resp := types.ApiResponse{
		Success:    true,
		StatusCode: statusCode,
		Data:       result,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *SagaHandler) CancelSaga(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "saga.cancel")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "missing saga id in path", start, reqID, corrID)
		return
	}
	sagaID := parts[3]

	if tenantID == "" {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "x-tenant-id header is required", start, reqID, corrID)
		return
	}

	result, err := h.service.CancelSaga(ctx, tenantID, sagaID)
	if err != nil {
		observability.RecordError(span, err)
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID)
		return
	}

	w.WriteHeader(http.StatusOK)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusOK,
		Data:       result,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *SagaHandler) GetSaga(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "saga.get")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "missing saga id in path", start, reqID, corrID)
		return
	}
	sagaID := parts[3]

	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenantId")
	}

	result, err := h.service.GetSaga(ctx, tenantID, sagaID)
	if err != nil {
		observability.RecordError(span, err)
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID)
		return
	}

	w.WriteHeader(http.StatusOK)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusOK,
		Data:       result,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *SagaHandler) ListSagas(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "saga.list")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenantId")
	}

	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	results, err := h.service.ListSagas(ctx, tenantID, limit, offset)
	if err != nil {
		observability.RecordError(span, err)
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID)
		return
	}

	w.WriteHeader(http.StatusOK)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusOK,
		Data:       results,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *SagaHandler) CreateSaga(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	traceparent := r.Header.Get(constants.HeaderTraceparent)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)

	var input sagaContracts.SagaInstance
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID)
		return
	}

	err := h.service.CreateSaga(r.Context(), &input)
	if err != nil {
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID)
		return
	}

	w.WriteHeader(http.StatusCreated)
	if traceparent != "" {
		w.Header().Set(constants.HeaderTraceparent, traceparent)
	}
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusCreated,
		Data:       input,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *SagaHandler) writeError(w http.ResponseWriter, code int, errCode string, msg string, start time.Time, reqID string, corrID string) {
	w.WriteHeader(code)
	resp := types.ApiResponse{
		Success:    false,
		StatusCode: code,
		Error: &types.ResponseError{
			Code:      errCode,
			Message:   msg,
			Retryable: code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable,
		},
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

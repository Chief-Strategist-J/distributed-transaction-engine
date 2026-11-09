package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	contracts "distributed-transaction-engine/src/features/compensation-retry/contracts"
	compensationretry "distributed-transaction-engine/src/features/compensation-retry"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
	"github.com/google/uuid"
)

type CompensationRetryHandler struct {
	service *compensationretry.CompensationRetryService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewCompensationRetryHandler(service *compensationretry.CompensationRetryService) *CompensationRetryHandler {
	return &CompensationRetryHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("compensation-retry-handler"),
	}
}

func (h *CompensationRetryHandler) Evaluate(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "compensation_retry.api_evaluate")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.EvaluateCompensationRetryInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	decision, err := h.service.Evaluate(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, decision, start)
}

func (h *CompensationRetryHandler) GetRecord(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "compensation_retry.api_get_record")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		tenantID = "default"
	}

	sagaIDStr := r.URL.Query().Get("sagaId")
	if sagaIDStr == "" {
		h.writeError(w, r, appErrors.NewValidationError("sagaId is required"), start)
		return
	}
	sagaUUID, err := uuid.Parse(sagaIDStr)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("sagaId must be a valid uuid"), start)
		return
	}

	stepID := r.URL.Query().Get("stepId")
	if stepID == "" {
		h.writeError(w, r, appErrors.NewValidationError("stepId is required"), start)
		return
	}

	record, err := h.service.GetRecord(ctx, tenantID, sagaUUID, stepID)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *CompensationRetryHandler) PauseStepType(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "compensation_retry.api_pause_step_type")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	stepType := r.URL.Query().Get("stepType")
	if stepType == "" {
		h.writeError(w, r, appErrors.NewValidationError("stepType is required"), start)
		return
	}
	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		tenantID = "default"
	}

	if err := h.service.PauseStepType(ctx, tenantID, stepType); err != nil {
		h.writeError(w, r, err, start)
		return
	}

	data := map[string]string{
		"stepType": stepType,
		"status":   "PAUSED",
	}
	h.writeResponse(w, r, http.StatusOK, data, start)
}

func (h *CompensationRetryHandler) ResumeStepType(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "compensation_retry.api_resume_step_type")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	stepType := r.URL.Query().Get("stepType")
	if stepType == "" {
		h.writeError(w, r, appErrors.NewValidationError("stepType is required"), start)
		return
	}
	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		tenantID = "default"
	}

	if err := h.service.ResumeStepType(ctx, tenantID, stepType); err != nil {
		h.writeError(w, r, err, start)
		return
	}

	data := map[string]string{
		"stepType": stepType,
		"status":   "RESUMED",
	}
	h.writeResponse(w, r, http.StatusOK, data, start)
}

func (h *CompensationRetryHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *CompensationRetryHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(types.ApiResponse{
		Success:    statusCode < 400,
		StatusCode: statusCode,
		Data:       data,
		Meta: types.ResponseMeta{
			RequestID:       r.Header.Get(constants.HeaderXRequestID),
			CorrelationID:   r.Header.Get(constants.HeaderXCorrelationID),
			CausationID:     r.Header.Get(constants.HeaderXCausationID),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	})
}

func (h *CompensationRetryHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
	statusCode := http.StatusInternalServerError
	errorCode := constants.ErrCodeInternalError
	retryable := false

	if appErr, ok := err.(*appErrors.ApplicationError); ok {
		statusCode = appErr.StatusCode
		errorCode = appErr.CanonicalCode
		retryable = appErr.Retryable
	}

	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(types.ApiResponse{
		Success:    false,
		StatusCode: statusCode,
		Error: &types.ResponseError{
			Code:      errorCode,
			Message:   err.Error(),
			Retryable: retryable,
		},
		Meta: types.ResponseMeta{
			RequestID:       r.Header.Get(constants.HeaderXRequestID),
			CorrelationID:   r.Header.Get(constants.HeaderXCorrelationID),
			CausationID:     r.Header.Get(constants.HeaderXCausationID),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	})
}


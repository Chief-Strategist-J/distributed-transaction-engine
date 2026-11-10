package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	compensationengine "distributed-transaction-engine/src/features/compensation-engine"
	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type CompensationHandler struct {
	service *compensationengine.Service
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewCompensationHandler(service *compensationengine.Service) *CompensationHandler {
	return &CompensationHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("compensation-handler"),
	}
}

func (h *CompensationHandler) TriggerCompensation(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "compensation_handler.trigger")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.TriggerCompensationInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid request payload"), start)
		return
	}

	plan, err := h.service.TriggerCompensation(ctx, &input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusCreated, plan, start)
}

func (h *CompensationHandler) CompleteStep(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "compensation_handler.complete_step")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.CompleteStepCompensationInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid request payload"), start)
		return
	}

	if err := h.service.CompleteStepCompensation(ctx, &input); err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, map[string]string{"status": "recorded"}, start)
}

func (h *CompensationHandler) GetPlan(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "compensation_handler.get_plan")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.Header.Get("X-Tenant-Id")
	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenantId")
	}

	sagaID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/compensation/plan/"), "/")

	plan, err := h.service.GetCompensationPlan(ctx, tenantID, sagaID)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, plan, start)
}

func (h *CompensationHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *CompensationHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}, start time.Time) {
	w.WriteHeader(statusCode)
	observability.NoteSwallowed(json.NewEncoder(w).Encode(types.ApiResponse{
		Success:    true,
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
	}))
}

func (h *CompensationHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
	statusCode := http.StatusInternalServerError
	errorCode := constants.ErrCodeInternalError
	retryable := false

	if appErr, ok := err.(*appErrors.ApplicationError); ok {
		statusCode = appErr.StatusCode
		errorCode = appErr.CanonicalCode
		retryable = appErr.Retryable
	}

	w.WriteHeader(statusCode)
	observability.NoteSwallowed(json.NewEncoder(w).Encode(types.ApiResponse{
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
	}))
}

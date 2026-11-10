package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	reconciliationengine "distributed-transaction-engine/src/features/reconciliation-engine"
	contracts "distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
	"github.com/google/uuid"
)

type ReconciliationHandler struct {
	service *reconciliationengine.ReconciliationService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewReconciliationHandler(service *reconciliationengine.ReconciliationService) *ReconciliationHandler {
	return &ReconciliationHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("reconciliation-handler"),
	}
}

func (h *ReconciliationHandler) Evaluate(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "reconciliation.api_evaluate")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.EvaluateReconciliationInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	res, err := h.service.Evaluate(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeSuccess(w, r, http.StatusOK, res, start)
}

func (h *ReconciliationHandler) ListAnomalies(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "reconciliation.api_list_anomalies")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		h.writeError(w, r, appErrors.NewValidationError("tenantId is required"), start)
		return
	}

	var statusFilter *contracts.ReconciliationStatus
	statusParam := r.URL.Query().Get("status")
	if statusParam != "" {
		st := contracts.ReconciliationStatus(statusParam)
		statusFilter = &st
	}

	anomalies, err := h.service.ListAnomalies(ctx, tenantID, statusFilter)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeSuccess(w, r, http.StatusOK, anomalies, start)
}

func (h *ReconciliationHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceparent)
	if traceID == "" {
		traceID = uuid.New().String()
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Trace-Id", traceID)
}

func (h *ReconciliationHandler) writeSuccess(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
	envelope := types.ApiResponse{
		Success:    true,
		StatusCode: statusCode,
		Data:       data,
		Meta: types.ResponseMeta{
			RequestID:       r.Header.Get(constants.HeaderXRequestID),
			CorrelationID:   r.Header.Get(constants.HeaderXCorrelationID),
			Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	w.WriteHeader(statusCode)
	observability.NoteSwallowed(json.NewEncoder(w).Encode(envelope))
}

func (h *ReconciliationHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
	statusCode := http.StatusInternalServerError
	canonicalCode := "INTERNAL_ERROR"
	retryable := false

	if appErr, ok := err.(*appErrors.ApplicationError); ok {
		statusCode = appErr.StatusCode
		canonicalCode = appErr.CanonicalCode
		retryable = appErr.Retryable
	}

	envelope := types.ApiResponse{
		Success:    false,
		StatusCode: statusCode,
		Error: &types.ResponseError{
			Code:      canonicalCode,
			Message:   err.Error(),
			Retryable: retryable,
		},
		Meta: types.ResponseMeta{
			RequestID:       r.Header.Get(constants.HeaderXRequestID),
			CorrelationID:   r.Header.Get(constants.HeaderXCorrelationID),
			Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	w.WriteHeader(statusCode)
	observability.NoteSwallowed(json.NewEncoder(w).Encode(envelope))
}

package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	occ "distributed-transaction-engine/src/features/optimistic-concurrency-control"
	contracts "distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"

	"github.com/google/uuid"
)

type OCCHandler struct {
	service *occ.OCCService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewOCCHandler(service *occ.OCCService) *OCCHandler {
	return &OCCHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("occ-handler"),
	}
}

func (h *OCCHandler) Mutate(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "occ.api_mutate")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.CASMutationInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	maxRetries := 5
	if mrStr := r.URL.Query().Get("maxRetries"); mrStr != "" {
		if parsed, err := strconv.Atoi(mrStr); err == nil && parsed > 0 {
			maxRetries = parsed
		}
	}

	res, err := h.service.MutateWithOCC(ctx, input, maxRetries)
	if err != nil {
		if res != nil {
			h.writeErrorWithData(w, r, err, res, start)
			return
		}
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *OCCHandler) ClientEdit(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "occ.api_client_edit")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.ClientEditInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	res, err := h.service.ApplyClientEdit(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *OCCHandler) Diagnose(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "occ.api_diagnose")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		h.writeError(w, r, appErrors.NewValidationError("tenantId query parameter is required"), start)
		return
	}

	idStr := r.URL.Query().Get("sagaId")
	sagaID, err := uuid.Parse(idStr)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid sagaId parameter: "+err.Error()), start)
		return
	}

	state, err := h.service.DiagnoseAggregate(ctx, tenantID, sagaID)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, state, start)
}

func (h *OCCHandler) Strategy(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "occ.api_strategy")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		h.writeError(w, r, appErrors.NewValidationError("tenantId query parameter is required"), start)
		return
	}

	idStr := r.URL.Query().Get("sagaId")
	sagaID, err := uuid.Parse(idStr)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid sagaId parameter: "+err.Error()), start)
		return
	}

	strat, err := h.service.EvaluateStrategy(ctx, tenantID, sagaID)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, map[string]any{
		"strategy": strat,
	}, start)
}

func (h *OCCHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *OCCHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
	w.WriteHeader(statusCode)
	observability.NoteSwallowed(json.NewEncoder(w).Encode(types.ApiResponse{
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
	}))
}

func (h *OCCHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
	h.writeErrorWithData(w, r, err, nil, start)
}

func (h *OCCHandler) writeErrorWithData(w http.ResponseWriter, r *http.Request, err error, data any, start time.Time) {
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
		Data:       data,
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

package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	contracts "distributed-transaction-engine/src/features/fencing-tokens/contracts"
	fencingtokens "distributed-transaction-engine/src/features/fencing-tokens"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type FencingTokensHandler struct {
	service *fencingtokens.FencingTokensService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewFencingTokensHandler(service *fencingtokens.FencingTokensService) *FencingTokensHandler {
	return &FencingTokensHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("fencing-tokens-handler"),
	}
}

func (h *FencingTokensHandler) Acquire(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "fencing.api_acquire")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.AcquireLeaseInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	lease, err := h.service.AcquireLease(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusCreated, lease, start)
}

func (h *FencingTokensHandler) Renew(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "fencing.api_renew")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.RenewLeaseInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	lease, err := h.service.RenewLease(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, lease, start)
}

func (h *FencingTokensHandler) Release(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "fencing.api_release")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.ReleaseLeaseInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if err := h.service.ReleaseLease(ctx, input); err != nil {
		h.writeError(w, r, err, start)
		return
	}

	data := map[string]string{
		"resourceId": input.ResourceID,
		"status":     "RELEASED",
	}
	h.writeResponse(w, r, http.StatusOK, data, start)
}

func (h *FencingTokensHandler) Barrier(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "fencing.api_barrier")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.FencedBarrierInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	decision, err := h.service.ValidateAndFence(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	if !decision.Allowed {
		h.writeResponse(w, r, http.StatusConflict, decision, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, decision, start)
}

func (h *FencingTokensHandler) GetAudits(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "fencing.api_get_audits")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	resourceID := r.URL.Query().Get("resourceId")
	if resourceID == "" {
		h.writeError(w, r, appErrors.NewValidationError("resourceId query parameter is required"), start)
		return
	}

	limit := 10
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	audits, err := h.service.GetAudits(ctx, tenantID, resourceID, limit)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, audits, start)
}

func (h *FencingTokensHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *FencingTokensHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *FencingTokensHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

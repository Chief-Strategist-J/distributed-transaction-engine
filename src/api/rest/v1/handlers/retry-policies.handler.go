package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	contracts "distributed-transaction-engine/src/features/retry-policies/contracts"
	retrypolicies "distributed-transaction-engine/src/features/retry-policies"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type RetryPoliciesHandler struct {
	service *retrypolicies.RetryPolicyService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewRetryPoliciesHandler(service *retrypolicies.RetryPolicyService) *RetryPoliciesHandler {
	return &RetryPoliciesHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("retry-policies-handler"),
	}
}

func (h *RetryPoliciesHandler) Evaluate(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "retry_policies.api_evaluate")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.EvaluateRetryInput
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

func (h *RetryPoliciesHandler) GetBreakerStatus(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "retry_policies.api_breaker_status")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	depID := r.URL.Query().Get("dependencyId")
	if depID == "" {
		h.writeError(w, r, appErrors.NewValidationError("dependencyId is required"), start)
		return
	}

	state := h.service.GetBreakerState(ctx, depID)
	data := map[string]interface{}{
		"dependencyId": depID,
		"state":        string(state),
	}
	h.writeResponse(w, r, http.StatusOK, data, start)
}

func (h *RetryPoliciesHandler) ResetBreaker(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "retry_policies.api_reset_breaker")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	depID := r.URL.Query().Get("dependencyId")
	if depID == "" {
		h.writeError(w, r, appErrors.NewValidationError("dependencyId is required"), start)
		return
	}

	h.service.ResetBreaker(ctx, depID)
	data := map[string]interface{}{
		"dependencyId": depID,
		"status":       "RESET",
	}
	h.writeResponse(w, r, http.StatusOK, data, start)
}

func (h *RetryPoliciesHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *RetryPoliciesHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}, start time.Time) {
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(types.ApiResponse{
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
	})
}

func (h *RetryPoliciesHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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


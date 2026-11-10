package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	tracing "distributed-transaction-engine/src/features/distributed-tracing"
	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
	"github.com/google/uuid"
)

type DistributedTracingHandler struct {
	service *tracing.DistributedTracingService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewDistributedTracingHandler(service *tracing.DistributedTracingService) *DistributedTracingHandler {
	return &DistributedTracingHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("distributed-tracing-handler"),
	}
}

func (h *DistributedTracingHandler) StartTrace(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dt.api_start_trace")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.Header.Get(constants.HeaderXTenantID)
	traceparentHeader := r.Header.Get(constants.HeaderTraceparent)
	baggageHeader := r.Header.Get("baggage")

	corrID := uuid.New()
	if rawCorr := r.Header.Get(constants.HeaderXCorrelationID); rawCorr != "" {
		if parsed, err := uuid.Parse(rawCorr); err == nil {
			corrID = parsed
		}
	}

	input := tracing.StartTraceInput{
		TenantID:          tenantID,
		TraceparentHeader: traceparentHeader,
		BaggageHeader:     baggageHeader,
		SamplingQuery: contracts.EvaluateSamplingQuery{
			TenantID:       tenantID,
			CorrelationID:  corrID,
			IsVIP:          r.Header.Get("X-VIP-Tenant") == "true",
			IsError:        false,
			IsCompensation: false,
		},
	}

	res, err := h.service.StartTrace(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *DistributedTracingHandler) RecordSpan(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dt.api_record_span")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.RecordSpanCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}

	res, err := h.service.RecordSpan(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusCreated, res, start)
}

func (h *DistributedTracingHandler) GetSpans(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dt.api_get_spans")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.Header.Get(constants.HeaderXTenantID)
	traceID := r.URL.Query().Get("trace_id")

	res, err := h.service.GetSpansByTraceID(ctx, tenantID, traceID)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *DistributedTracingHandler) RegisterAsyncLink(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dt.api_register_async_link")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.RegisterAsyncLinkCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}

	res, err := h.service.RegisterAsyncLink(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusCreated, res, start)
}

func (h *DistributedTracingHandler) ResolveAsyncLink(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dt.api_resolve_async_link")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var query contracts.ResolveAsyncLinkQuery
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&query); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if query.TenantID == "" {
		query.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}

	res, err := h.service.ResolveAsyncLink(ctx, query)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *DistributedTracingHandler) ActivateDeepSession(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dt.api_activate_deep_session")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.ActivateDeepSessionCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}

	res, err := h.service.ActivateDeepSession(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusCreated, res, start)
}

func (h *DistributedTracingHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if tp := r.Header.Get(constants.HeaderTraceparent); tp != "" {
		w.Header().Set(constants.HeaderTraceparent, tp)
	}
	if tid := r.Header.Get(constants.HeaderXTenantID); tid != "" {
		w.Header().Set(constants.HeaderXTenantID, tid)
	}
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *DistributedTracingHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *DistributedTracingHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

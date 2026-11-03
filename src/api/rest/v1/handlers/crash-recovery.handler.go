package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	crashRecovery "distributed-transaction-engine/src/features/crash-recovery"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"

	"go.opentelemetry.io/otel/trace"
)

type TriggerSweepInput struct {
	BatchLimit int `json:"batchLimit,omitempty"`
}

type CrashRecoveryHandler struct {
	service *crashRecovery.CrashRecoveryService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewCrashRecoveryHandler(service *crashRecovery.CrashRecoveryService) *CrashRecoveryHandler {
	return &CrashRecoveryHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("crash-recovery-handler"),
	}
}

func (h *CrashRecoveryHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "crash_recovery.api_status")
	defer span.End()

	start := time.Now()
	h.setStandardHeaders(w, r)

	cfg := h.service.GetConfig()
	data := map[string]interface{}{
		"incarnationId":      cfg.IncarnationID,
		"scanIntervalMs":     cfg.ScanInterval.Milliseconds(),
		"batchSize":          cfg.BatchSize,
		"leaseDurationMs":    cfg.LeaseDuration.Milliseconds(),
		"maxTakeovers":       cfg.MaxTakeovers,
		"selfFenceMarginMs":  cfg.SelfFenceMargin.Milliseconds(),
		"maxConcurrentOwned": cfg.MaxConcurrentOwned,
		"status":             "ACTIVE",
	}

	h.writeSuccess(w, r, http.StatusOK, data, start)
	h.logger.Info(ctx, "crash recovery status retrieved",
		observability.String("incarnation_id", cfg.IncarnationID),
	)
}

func (h *CrashRecoveryHandler) GetOrphans(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "crash_recovery.api_get_orphans")
	defer span.End()

	start := time.Now()
	h.setStandardHeaders(w, r)

	limit := 50
	limitQuery := r.URL.Query().Get("limit")
	if limitQuery != "" {
		if parsed, err := strconv.Atoi(limitQuery); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	orphans, err := h.service.GetOrphans(ctx, limit)
	if err != nil {
		h.writeFailure(w, r, span, err, start)
		return
	}

	h.writeSuccess(w, r, http.StatusOK, orphans, start)
	h.logger.Info(ctx, "crash recovery orphans queried",
		observability.Int("orphan_count", len(orphans)),
	)
}

func (h *CrashRecoveryHandler) TriggerSweep(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "crash_recovery.api_trigger_sweep")
	defer span.End()

	start := time.Now()
	h.setStandardHeaders(w, r)

	if r.Body != nil && r.ContentLength > 0 {
		var input TriggerSweepInput
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			h.writeFailure(w, r, span, appErrors.NewValidationError(err.Error()), start)
			return
		}
	}

	reclaimed, err := h.service.SweepAndReclaim(ctx)
	if err != nil {
		h.writeFailure(w, r, span, err, start)
		return
	}

	data := map[string]interface{}{
		"reclaimedCount": reclaimed,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}
	h.writeSuccess(w, r, http.StatusOK, data, start)
}

func (h *CrashRecoveryHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if traceparent := r.Header.Get(constants.HeaderTraceparent); traceparent != "" {
		w.Header().Set(constants.HeaderTraceparent, traceparent)
	}
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *CrashRecoveryHandler) resolveErrorDetails(err error) (int, string, string) {
	if appErr, ok := err.(*appErrors.ApplicationError); ok {
		return appErr.StatusCode, appErr.CanonicalCode, appErr.Message
	}
	return http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error()
}

func (h *CrashRecoveryHandler) writeSuccess(
	w http.ResponseWriter,
	r *http.Request,
	statusCode int,
	data interface{},
	start time.Time,
) {
	w.WriteHeader(statusCode)
	resp := types.ApiResponse{
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
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *CrashRecoveryHandler) writeFailure(
	w http.ResponseWriter,
	r *http.Request,
	span trace.Span,
	err error,
	start time.Time,
) {
	observability.RecordError(span, err)
	statusCode, code, msg := h.resolveErrorDetails(err)
	w.WriteHeader(statusCode)
	resp := types.ApiResponse{
		Success:    false,
		StatusCode: statusCode,
		Error: &types.ResponseError{
			Code:      code,
			Message:   msg,
			Retryable: statusCode >= 500,
		},
		Meta: types.ResponseMeta{
			RequestID:       r.Header.Get(constants.HeaderXRequestID),
			CorrelationID:   r.Header.Get(constants.HeaderXCorrelationID),
			CausationID:     r.Header.Get(constants.HeaderXCausationID),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

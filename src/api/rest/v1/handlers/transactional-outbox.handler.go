package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	transactionaloutbox "distributed-transaction-engine/src/features/transactional-outbox"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type RelayRequestDTO struct {
	TenantID      string `json:"tenantId"`
	Topic         string `json:"topic"`
	WorkerID      string `json:"workerId"`
	BatchLimit    int    `json:"batchLimit"`
	LeaseDuration string `json:"leaseDuration"`
}

type OutboxHandler struct {
	service *transactionaloutbox.Service
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewOutboxHandler(service *transactionaloutbox.Service) *OutboxHandler {
	return &OutboxHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("outbox-handler"),
	}
}

func (h *OutboxHandler) RelayPending(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "outbox_handler.relay_pending")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var req RelayRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid request payload"), start)
		return
	}

	leaseDur := 30 * time.Second
	if req.LeaseDuration != "" {
		if parsed, err := time.ParseDuration(req.LeaseDuration); err == nil {
			leaseDur = parsed
		}
	}

	if req.BatchLimit <= 0 {
		req.BatchLimit = 50
	}

	count, err := h.service.RelayPendingEvents(ctx, req.TenantID, req.Topic, req.WorkerID, req.BatchLimit, leaseDur)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	res := map[string]interface{}{
		"publishedCount": count,
		"topic":          req.Topic,
		"workerId":       req.WorkerID,
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *OutboxHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *OutboxHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}, start time.Time) {
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

func (h *OutboxHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

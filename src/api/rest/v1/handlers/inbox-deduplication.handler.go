package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	inboxdeduplication "distributed-transaction-engine/src/features/inbox-deduplication"
	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type InboxDeduplicationHandler struct {
	service *inboxdeduplication.Service
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewInboxDeduplicationHandler(service *inboxdeduplication.Service) *InboxDeduplicationHandler {
	return &InboxDeduplicationHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("inbox-deduplication-handler"),
	}
}

func (h *InboxDeduplicationHandler) IngestMessage(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "inbox_deduplication.ingest")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.IngestMessageInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid request payload"), start)
		return
	}

	decision, err := h.service.ProcessIncomingMessage(ctx, &input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	statusCode := http.StatusOK
	if decision.Outcome == contracts.OutcomeApplied {
		statusCode = http.StatusCreated
	}

	h.writeResponse(w, r, statusCode, decision, start)
}

func (h *InboxDeduplicationHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *InboxDeduplicationHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}, start time.Time) {
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

func (h *InboxDeduplicationHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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


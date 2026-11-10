package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	idempotencykeys "distributed-transaction-engine/src/features/idempotency-keys"
	contracts "distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type CommitOutcomeInput struct {
	TenantID          string                     `json:"tenantId"`
	ID                string                     `json:"id"`
	State             contracts.IdempotencyState `json:"state"`
	Snapshot          contracts.ResponseSnapshot `json:"snapshot"`
	ResourceReference string                     `json:"resourceReference,omitempty"`
}

type IdempotencyKeysHandler struct {
	service *idempotencykeys.IdempotencyService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewIdempotencyKeysHandler(service *idempotencykeys.IdempotencyService) *IdempotencyKeysHandler {
	return &IdempotencyKeysHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("idempotency-keys-handler"),
	}
}

func (h *IdempotencyKeysHandler) Acquire(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "idempotency_keys.api_acquire")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.IngestIdempotencyInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	result, err := h.service.Ingest(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	statusCode := http.StatusOK
	if result.Outcome == contracts.OutcomeAcquired {
		statusCode = http.StatusCreated
	} else if result.Outcome == contracts.OutcomeReplay {
		w.Header().Set("Idempotent-Replayed", "true")
	}

	h.writeResponse(w, r, statusCode, result, start)
}

func (h *IdempotencyKeysHandler) Commit(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "idempotency_keys.api_commit")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input CommitOutcomeInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	var err error
	if input.State == contracts.StateCompleted {
		err = h.service.CommitSuccess(ctx, input.TenantID, input.ID, input.Snapshot, input.ResourceReference)
	} else {
		err = h.service.CommitRejection(ctx, input.TenantID, input.ID, input.Snapshot)
	}

	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, map[string]string{"status": "COMMITTED"}, start)
}

func (h *IdempotencyKeysHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *IdempotencyKeysHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}, start time.Time) {
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

func (h *IdempotencyKeysHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

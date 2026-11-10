package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	dlq "distributed-transaction-engine/src/features/dead-letter-queue"
	contracts "distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"

	"github.com/google/uuid"
)

type DeadLetterQueueHandler struct {
	service *dlq.DeadLetterService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewDeadLetterQueueHandler(service *dlq.DeadLetterService) *DeadLetterQueueHandler {
	return &DeadLetterQueueHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("dead-letter-queue-handler"),
	}
}

func (h *DeadLetterQueueHandler) Admit(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_admit")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.AdmitDLQInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	record, err := h.service.AdmitRecord(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusCreated, record, start)
}

func (h *DeadLetterQueueHandler) Triage(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_triage")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.TriageInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	record, err := h.service.TriageRecord(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *DeadLetterQueueHandler) Retry(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_retry")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.RetryActionInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	sagaState := r.URL.Query().Get("sagaState")
	if sagaState == "" {
		sagaState = "RUNNING"
	}
	stepStr := r.URL.Query().Get("stepIndex")
	stepIndex, _ := strconv.Atoi(stepStr)

	record, err := h.service.RetryRecord(ctx, input, sagaState, stepIndex)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *DeadLetterQueueHandler) ResolveEvidence(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_resolve_evidence")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.ResolveEvidenceInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	record, err := h.service.ResolveEvidence(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *DeadLetterQueueHandler) CorrectiveAction(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_corrective_action")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.CorrectiveActionInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	record, err := h.service.ApplyCorrectiveAction(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *DeadLetterQueueHandler) WriteOff(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_write_off")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.WriteOffInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	record, err := h.service.WriteOffRecord(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *DeadLetterQueueHandler) ResolveSaga(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_resolve_saga")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.ResolveSagaActionInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	record, err := h.service.ResolveSagaAction(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *DeadLetterQueueHandler) GetRecord(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_get_record")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		h.writeError(w, r, appErrors.NewValidationError("tenantId query parameter is required"), start)
		return
	}

	idStr := r.URL.Query().Get("id")
	recID, err := uuid.Parse(idStr)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid id parameter: "+err.Error()), start)
		return
	}

	record, err := h.service.GetRecord(ctx, tenantID, recID)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, record, start)
}

func (h *DeadLetterQueueHandler) ListRecords(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dlq.api_list_records")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		h.writeError(w, r, appErrors.NewValidationError("tenantId query parameter is required"), start)
		return
	}

	var statusPtr *contracts.RecordStatus
	if st := r.URL.Query().Get("status"); st != "" {
		typed := contracts.RecordStatus(st)
		statusPtr = &typed
	}

	limit := 50
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	records, err := h.service.ListRecords(ctx, tenantID, statusPtr, limit)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, records, start)
}

func (h *DeadLetterQueueHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *DeadLetterQueueHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *DeadLetterQueueHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

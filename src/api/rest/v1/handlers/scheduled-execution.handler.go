package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	scheduledexecution "distributed-transaction-engine/src/features/scheduled-execution"
	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
	"github.com/google/uuid"
)

type ScheduledExecutionHandler struct {
	service *scheduledexecution.ScheduledExecutionService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewScheduledExecutionHandler(service *scheduledexecution.ScheduledExecutionService) *ScheduledExecutionHandler {
	return &ScheduledExecutionHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("scheduled-execution-handler"),
	}
}

func (h *ScheduledExecutionHandler) CreateSchedule(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "scheduled_execution.api_create")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.CreateScheduleInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	res, err := h.service.CreateSchedule(ctx, input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeSuccess(w, r, http.StatusCreated, res, start)
}

func (h *ScheduledExecutionHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceparent)
	if traceID == "" {
		traceID = uuid.New().String()
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Trace-Id", traceID)
}

func (h *ScheduledExecutionHandler) writeSuccess(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *ScheduledExecutionHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

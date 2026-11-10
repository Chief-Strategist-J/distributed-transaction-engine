package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	parallelexecution "distributed-transaction-engine/src/features/parallel-execution"
	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type ParallelExecutionHandler struct {
	service *parallelexecution.Service
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewParallelExecutionHandler(service *parallelexecution.Service) *ParallelExecutionHandler {
	return &ParallelExecutionHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("parallel-execution-handler"),
	}
}

func (h *ParallelExecutionHandler) StartGroup(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "parallel_execution.start")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.StartParallelGroupInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid request payload"), start)
		return
	}

	group, err := h.service.StartParallelGroup(ctx, &input)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusCreated, group, start)
}

func (h *ParallelExecutionHandler) CompleteBranch(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "parallel_execution.complete_branch")
	defer span.End()

	h.setStandardHeaders(w, r)
	start := time.Now()

	var input contracts.CompleteBranchInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, r, appErrors.NewValidationError("invalid request payload"), start)
		return
	}

	if input.BranchID == "" && input.BranchPublicID != "" {
		input.BranchID = input.BranchPublicID
	}

	err := h.service.HandleBranchCompletion(ctx, input.TenantID, input.GroupID, input.BranchID, input.IsSuccess, input.Output, input.ErrorMessage)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, map[string]string{"status": "completed"}, start)
}

func (h *ParallelExecutionHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if reqID := r.Header.Get(constants.HeaderXRequestID); reqID != "" {
		w.Header().Set(constants.HeaderXRequestID, reqID)
	}
	if corrID := r.Header.Get(constants.HeaderXCorrelationID); corrID != "" {
		w.Header().Set(constants.HeaderXCorrelationID, corrID)
	}
}

func (h *ParallelExecutionHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data interface{}, start time.Time) {
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

func (h *ParallelExecutionHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

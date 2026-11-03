package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type TransactionStateMachineHandler struct {
	service *transactionStateMachine.Service
	tracer  *observability.DefaultTracer
	logger  *observability.LokiLogger
}

func NewTransactionStateMachineHandler(s *transactionStateMachine.Service) *TransactionStateMachineHandler {
	return &TransactionStateMachineHandler{
		service: s,
		tracer:  observability.NewTracer("transaction-state-machine-handler"),
		logger:  observability.GetLogger(),
	}
}

func (h *TransactionStateMachineHandler) InitializeState(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "state_machine.initialize")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	traceparent := r.Header.Get(constants.HeaderTraceparent)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)

	var input transactionStateMachine.InitializeInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		observability.RecordError(span, err)
		h.logger.Warn(ctx, "failed to decode initialize state payload", observability.String("error", err.Error()))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID)
		return
	}

	result, err := h.service.InitializeState(ctx, input)
	if err != nil {
		observability.RecordError(span, err)
		h.logger.Error(ctx, "state initialization failed", observability.String("error", err.Error()))
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID)
		return
	}

	w.Header().Set(constants.HeaderTraceparent, traceparent)
	w.WriteHeader(http.StatusCreated)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusCreated,
		Data:       result,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *TransactionStateMachineHandler) TransitionState(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "state_machine.transition")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	traceparent := r.Header.Get(constants.HeaderTraceparent)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)

	var input transactionStateMachine.TransitionInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		observability.RecordError(span, err)
		h.logger.Warn(ctx, "failed to decode transition state payload", observability.String("error", err.Error()))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID)
		return
	}

	result, err := h.service.TransitionState(ctx, input)
	if err != nil {
		observability.RecordError(span, err)
		h.logger.Error(ctx, "state transition failed", observability.String("error", err.Error()))
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID)
		return
	}

	w.Header().Set(constants.HeaderTraceparent, traceparent)
	w.WriteHeader(http.StatusOK)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusOK,
		Data:       result,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *TransactionStateMachineHandler) writeError(w http.ResponseWriter, status int, code, msg string, start time.Time, reqID, corrID string) {
	w.WriteHeader(status)
	resp := types.ApiResponse{
		Success:    false,
		StatusCode: status,
		Error: &types.ResponseError{
			Code:      code,
			Message:   msg,
			Retryable: status >= 500,
		},
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

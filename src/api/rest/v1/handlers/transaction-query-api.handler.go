package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	queryapi "distributed-transaction-engine/src/features/transaction-query-api"
	"distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type TransactionQueryHandler struct {
	service *queryapi.TransactionQueryService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewTransactionQueryHandler(service *queryapi.TransactionQueryService) *TransactionQueryHandler {
	return &TransactionQueryHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("transaction-query-handler"),
	}
}

func (h *TransactionQueryHandler) resolvePrincipal(r *http.Request) contracts.Principal {
	tenantID := r.Header.Get(constants.HeaderXTenantID)
	principalID := r.Header.Get("X-Principal-ID")
	if principalID == "" {
		principalID = "user_default"
	}
	audience := contracts.AudienceTenantUser
	if r.Header.Get("X-Operator-Role") == "sre_admin" {
		audience = contracts.AudienceOperator
	}
	return contracts.Principal{
		TenantID:    tenantID,
		PrincipalID: principalID,
		Audience:    audience,
	}
}

func (h *TransactionQueryHandler) SearchTransactions(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "tq.api_search")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	principal := h.resolvePrincipal(r)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("failed to read body: "+err.Error()), start)
		return
	}

	var req contracts.SearchRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			h.writeError(w, r, appErrors.NewValidationError("invalid search request format: "+err.Error()), start)
			return
		}
	}

	resp, err := h.service.SearchTransactions(ctx, principal, req)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, resp, start)
}

func (h *TransactionQueryHandler) AggregateTransactions(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "tq.api_aggregate")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	principal := h.resolvePrincipal(r)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("failed to read body: "+err.Error()), start)
		return
	}

	var req contracts.AggregateRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			h.writeError(w, r, appErrors.NewValidationError("invalid aggregate request format: "+err.Error()), start)
			return
		}
	}

	resp, err := h.service.AggregateTransactions(ctx, principal, req)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, resp, start)
}

func (h *TransactionQueryHandler) GetTransaction(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "tq.api_get_transaction")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	principal := h.resolvePrincipal(r)
	path := r.URL.Path
	publicID := strings.TrimPrefix(path, "/api/v1/transactions/")
	if publicID == "" {
		h.writeError(w, r, appErrors.NewValidationError("transaction public_id is required"), start)
		return
	}

	req := contracts.GetTransactionRequest{
		TransactionID: publicID,
	}

	resp, err := h.service.GetTransaction(ctx, principal, req)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, resp, start)
}

func (h *TransactionQueryHandler) GetTransactionTimeline(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "tq.api_timeline")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	principal := h.resolvePrincipal(r)
	path := r.URL.Path
	trimmed := strings.TrimPrefix(path, "/api/v1/transactions/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[0] == "" {
		h.writeError(w, r, appErrors.NewValidationError("transaction public_id is required"), start)
		return
	}
	publicID := parts[0]

	req := contracts.TimelineRequest{
		TransactionID: publicID,
	}

	resp, err := h.service.GetTransactionTimeline(ctx, principal, req)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, resp, start)
}

func (h *TransactionQueryHandler) CreateExport(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "tq.api_export")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	principal := h.resolvePrincipal(r)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("failed to read body: "+err.Error()), start)
		return
	}

	var req contracts.ExportRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			h.writeError(w, r, appErrors.NewValidationError("invalid export request format: "+err.Error()), start)
			return
		}
	}

	job, err := h.service.CreateExport(ctx, principal, req)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusAccepted, job, start)
}

func (h *TransactionQueryHandler) SavedQueries(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "tq.api_saved_queries")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	principal := h.resolvePrincipal(r)

	if r.Method == http.MethodGet {
		queries, err := h.service.ListSavedQueries(ctx, principal)
		if err != nil {
			h.writeError(w, r, err, start)
			return
		}
		h.writeResponse(w, r, http.StatusOK, queries, start)
		return
	}

	if r.Method == http.MethodPost {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.writeError(w, r, appErrors.NewValidationError("failed to read body: "+err.Error()), start)
			return
		}
		var req contracts.CreateSavedQueryRequest
		if err := json.Unmarshal(body, &req); err != nil {
			h.writeError(w, r, appErrors.NewValidationError("invalid saved query format: "+err.Error()), start)
			return
		}
		sq, err := h.service.CreateSavedQuery(ctx, principal, req)
		if err != nil {
			h.writeError(w, r, err, start)
			return
		}
		h.writeResponse(w, r, http.StatusCreated, sq, start)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (h *TransactionQueryHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
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

func (h *TransactionQueryHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *TransactionQueryHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

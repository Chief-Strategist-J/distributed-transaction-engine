package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	wv "distributed-transaction-engine/src/features/workflow-versioning"
	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
	"github.com/google/uuid"
)

type WorkflowVersioningHandler struct {
	service *wv.WorkflowVersioningService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewWorkflowVersioningHandler(service *wv.WorkflowVersioningService) *WorkflowVersioningHandler {
	return &WorkflowVersioningHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("workflow-versioning-handler"),
	}
}

func (h *WorkflowVersioningHandler) RegisterBundle(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "wv.api_register_bundle")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()
	var cmd contracts.RegisterBundleCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}
	enriched := h.enrichBundleCommand(cmd, r)
	res, err := h.service.RegisterBundle(ctx, enriched)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusCreated, res, start)
}

func (h *WorkflowVersioningHandler) enrichBundleCommand(cmd contracts.RegisterBundleCommand, r *http.Request) contracts.RegisterBundleCommand {
	cloned := cmd
	if cloned.TenantID == "" {
		cloned.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	if cloned.CorrelationID == uuid.Nil {
		if cid, err := uuid.Parse(r.Header.Get(constants.HeaderXCorrelationID)); err == nil {
			cloned.CorrelationID = cid
		}
	}
	return cloned
}

func (h *WorkflowVersioningHandler) ProposePromotion(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "wv.api_propose_promotion")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()
	var cmd contracts.ProposePromotionCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}
	cloned := cmd
	if cloned.TenantID == "" {
		cloned.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	res, err := h.service.ProposePromotion(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusAccepted, res, start)
}

func (h *WorkflowVersioningHandler) DecidePromotion(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "wv.api_decide_promotion")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()
	var cmd contracts.DecidePromotionCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}
	cloned := cmd
	if cloned.TenantID == "" {
		cloned.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	res, err := h.service.DecidePromotion(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *WorkflowVersioningHandler) ResolveVersion(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "wv.api_resolve_version")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()
	var query contracts.ResolveVersionQuery
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&query); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}
	cloned := query
	if cloned.TenantID == "" {
		cloned.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	res, err := h.service.ResolveVersion(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *WorkflowVersioningHandler) VerifyIntegrity(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "wv.api_verify_integrity")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()
	var query contracts.VerifyIntegrityQuery
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&query); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}
	cloned := query
	if cloned.TenantID == "" {
		cloned.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	res, err := h.service.VerifyBundleIntegrity(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *WorkflowVersioningHandler) MigrateSaga(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "wv.api_migrate_saga")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()
	var cmd contracts.MigrateSagaCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}
	cloned := cmd
	if cloned.TenantID == "" {
		cloned.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	res, err := h.service.MigrateSaga(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *WorkflowVersioningHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
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

func (h *WorkflowVersioningHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *WorkflowVersioningHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

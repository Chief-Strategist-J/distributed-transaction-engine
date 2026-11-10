package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	audittrail "distributed-transaction-engine/src/features/audit-trail"
	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
	"github.com/google/uuid"
)

type AuditTrailHandler struct {
	service *audittrail.AuditTrailService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewAuditTrailHandler(service *audittrail.AuditTrailService) *AuditTrailHandler {
	return &AuditTrailHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("audit-trail-handler"),
	}
}

func (h *AuditTrailHandler) CaptureEvent(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "audit.api_capture_event")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.CaptureOutboxEventCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	enriched := h.enrichCaptureCommand(cmd, r)
	res, err := h.service.CaptureOutboxEvent(ctx, enriched)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusAccepted, res, start)
}

func (h *AuditTrailHandler) ProcessChainer(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "audit.api_process_chainer")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.ProcessChainerBatchCommand
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
	if len(cloned.SecretKey) == 0 {
		cloned.SecretKey = []byte("audit_trail_engine_hmac_secret_key_32b!")
	}

	res, err := h.service.ProcessChainerBatch(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuditTrailHandler) CreateCheckpoint(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "audit.api_create_checkpoint")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.CreateCheckpointCommand
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
	if len(cloned.SigningKey) == 0 {
		cloned.SigningKey = []byte("audit_trail_engine_signing_secret_key!")
	}

	res, err := h.service.CreateCheckpoint(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusCreated, res, start)
}

func (h *AuditTrailHandler) VerifyChain(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "audit.api_verify_chain")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var query contracts.VerifyChainQuery
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
	if len(cloned.SecretKey) == 0 {
		cloned.SecretKey = []byte("audit_trail_engine_hmac_secret_key_32b!")
	}

	res, err := h.service.VerifyChain(ctx, cloned)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuditTrailHandler) ListEvents(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "audit.api_list_events")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.Header.Get(constants.HeaderXTenantID)
	if paramTenant := r.URL.Query().Get("tenant_id"); paramTenant != "" {
		tenantID = paramTenant
	}

	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	query := contracts.ListAuditEventsQuery{
		TenantID: tenantID,
		ActorID:  r.URL.Query().Get("actor_id"),
		Action:   r.URL.Query().Get("action"),
		Limit:    limit,
		MaskPII:  true,
	}

	res, err := h.service.ListEvents(ctx, query)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuditTrailHandler) enrichCaptureCommand(cmd contracts.CaptureOutboxEventCommand, r *http.Request) contracts.CaptureOutboxEventCommand {
	cloned := cmd
	if cloned.TenantID == "" {
		cloned.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	if cloned.CorrelationID == uuid.Nil {
		if cid, err := uuid.Parse(r.Header.Get(constants.HeaderXCorrelationID)); err == nil {
			cloned.CorrelationID = cid
		}
	}
	if cloned.ActorIP == "" {
		cloned.ActorIP = r.RemoteAddr
	}
	if cloned.OccurredAt.IsZero() {
		cloned.OccurredAt = time.Now().UTC()
	}
	return cloned
}

func (h *AuditTrailHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
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

func (h *AuditTrailHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *AuditTrailHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

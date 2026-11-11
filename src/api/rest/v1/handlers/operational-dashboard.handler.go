package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	dashboard "distributed-transaction-engine/src/features/operational-dashboard"
	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type OperationalDashboardHandler struct {
	service *dashboard.DashboardService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewOperationalDashboardHandler(service *dashboard.DashboardService) *OperationalDashboardHandler {
	return &OperationalDashboardHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("operational-dashboard-handler"),
	}
}

func (h *OperationalDashboardHandler) GetOverview(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dashboard.api_get_overview")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.Header.Get(constants.HeaderXTenantID)
	principalID := r.Header.Get(constants.HeaderXPrincipalID)
	if principalID == "" {
		principalID = "anonymous_operator"
	}

	scopeType := contracts.ScopeTenantAdmin
	if r.Header.Get(constants.HeaderXOperatorRole) == constants.OperatorRoleSREAdmin {
		scopeType = contracts.ScopeOperator
	} else if r.Header.Get("X-Impersonation-Ticket") != "" {
		scopeType = contracts.ScopeSupport
	}

	var impTicket *string
	if rawTicket := r.Header.Get("X-Impersonation-Ticket"); rawTicket != "" {
		impTicket = &rawTicket
	}

	req := &contracts.ConnectStreamRequest{
		PrincipalID:         principalID,
		ScopeType:           scopeType,
		ImpersonationTicket: impTicket,
		RequestedTenant:     r.URL.Query().Get("tenant"),
	}

	overview, err := h.service.GetOverview(ctx, tenantID, req, impTicket, r.RemoteAddr)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, overview, start)
}

func (h *OperationalDashboardHandler) StreamEvents(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dashboard.api_stream_events")
	defer span.End()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if tp := r.Header.Get(constants.HeaderTraceparent); tp != "" {
		w.Header().Set(constants.HeaderTraceparent, tp)
	}

	tenantID := r.Header.Get(constants.HeaderXTenantID)
	principalID := r.Header.Get(constants.HeaderXPrincipalID)
	if principalID == "" {
		principalID = "stream_operator"
	}

	scopeType := contracts.ScopeTenantAdmin
	if r.Header.Get(constants.HeaderXOperatorRole) == constants.OperatorRoleSREAdmin {
		scopeType = contracts.ScopeOperator
	} else if r.Header.Get("X-Impersonation-Ticket") != "" {
		scopeType = contracts.ScopeSupport
	}

	var impTicket *string
	if rawTicket := r.Header.Get("X-Impersonation-Ticket"); rawTicket != "" {
		impTicket = &rawTicket
	}

	req := &contracts.ConnectStreamRequest{
		PrincipalID:         principalID,
		ScopeType:           scopeType,
		ImpersonationTicket: impTicket,
		RequestedTenant:     r.URL.Query().Get("tenant"),
	}

	session, eventCh, err := h.service.RegisterStreamSession(ctx, tenantID, req, impTicket, r.RemoteAddr)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		h.writeError(w, r, err, time.Now())
		return
	}
	defer h.service.UnregisterStreamSession(r.Context(), session.TenantID, session.PublicID)

	snapshotEvent := fmt.Sprintf("event: connected\ndata: {\"session_id\":\"%s\",\"scope\":\"%s\",\"status\":\"ACTIVE\"}\n\n", session.PublicID, session.TenantID)
	_, swallowedErr1 := fmt.Fprint(w, snapshotEvent)
	observability.NoteSwallowed(swallowedErr1)
	flusher.Flush()

	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case <-heartbeatTicker.C:
			if err := h.service.Heartbeat(r.Context(), session.TenantID, session.PublicID); err != nil {
				revokedEvent := fmt.Sprintf("event: revoked\ndata: {\"reason\":\"%s\"}\n\n", err.Error())
				_, swallowedErr2 := fmt.Fprint(w, revokedEvent)
				observability.NoteSwallowed(swallowedErr2)
				flusher.Flush()
				return
			}
			hbEvent := fmt.Sprintf("event: heartbeat\ndata: {\"timestamp\":\"%s\"}\n\n", time.Now().UTC().Format(time.RFC3339))
			_, swallowedErr1 := fmt.Fprint(w, hbEvent)
			observability.NoteSwallowed(swallowedErr1)
			flusher.Flush()

		case event, open := <-eventCh:
			if !open {
				return
			}
			msg := fmt.Sprintf("id: %s\nevent: %s\ndata: %s\n\n", event.ID, event.Event, string(event.Data))
			_, swallowedErr2 := fmt.Fprint(w, msg)
			observability.NoteSwallowed(swallowedErr2)
			flusher.Flush()
		}
	}
}

func (h *OperationalDashboardHandler) PreviewAction(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dashboard.api_preview_action")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.Header.Get(constants.HeaderXTenantID)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("failed to read request body: "+err.Error()), start)
		return
	}

	req, err := contracts.ValidatePreviewActionRequest(body)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	resp, err := h.service.PreviewAction(ctx, tenantID, req, r.RemoteAddr)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, resp, start)
}

func (h *OperationalDashboardHandler) ExecuteAction(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "dashboard.api_execute_action")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	tenantID := r.Header.Get(constants.HeaderXTenantID)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("failed to read request body: "+err.Error()), start)
		return
	}

	req, err := contracts.ValidateExecuteActionRequest(body)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	resp, err := h.service.ExecuteAction(ctx, tenantID, req, r.RemoteAddr)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	h.writeResponse(w, r, http.StatusOK, resp, start)
}

func (h *OperationalDashboardHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
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

func (h *OperationalDashboardHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *OperationalDashboardHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

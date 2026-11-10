package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	auth "distributed-transaction-engine/src/features/authentication-authorization"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"distributed-transaction-engine/src/shared/types"
)

type AuthenticationAuthorizationHandler struct {
	service *auth.AuthenticationAuthorizationService
	logger  *observability.LokiLogger
	tracer  *observability.DefaultTracer
}

func NewAuthenticationAuthorizationHandler(service *auth.AuthenticationAuthorizationService) *AuthenticationAuthorizationHandler {
	return &AuthenticationAuthorizationHandler{
		service: service,
		logger:  observability.GetLogger(),
		tracer:  observability.NewTracer("authentication-authorization-handler"),
	}
}

func (h *AuthenticationAuthorizationHandler) AuthenticateToken(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_authenticate_token")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.AuthenticateTokenCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	res, err := h.service.AuthenticateToken(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuthenticationAuthorizationHandler) AuthenticateAPIKey(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_authenticate_api_key")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.AuthenticateAPIKeyCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	if cmd.ClientIP == "" {
		cmd.ClientIP = r.RemoteAddr
	}

	res, err := h.service.AuthenticateAPIKey(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuthenticationAuthorizationHandler) AuthenticateService(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_authenticate_service")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.AuthenticateServiceCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	res, err := h.service.AuthenticateService(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuthenticationAuthorizationHandler) AuthenticateWebhook(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_authenticate_webhook")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	rawBytes, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, r, appErrors.NewValidationError("cannot read webhook body"), start)
		return
	}

	var cmd contracts.AuthenticateWebhookCommand
	if err := json.Unmarshal(rawBytes, &cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	cmd.RawPayload = rawBytes
	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	if cmd.Signature == "" {
		cmd.Signature = r.Header.Get("X-Provider-Signature")
	}
	if cmd.Nonce == "" {
		cmd.Nonce = r.Header.Get("X-Provider-Nonce")
	}
	if cmd.Timestamp.IsZero() {
		cmd.Timestamp = time.Now().UTC()
	}

	res, err := h.service.AuthenticateWebhook(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuthenticationAuthorizationHandler) Authorize(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_authorize")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.AuthorizeRequestCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.ResourceTenantID == "" {
		cmd.ResourceTenantID = r.Header.Get(constants.HeaderXTenantID)
	}
	if cmd.Auth.TenantID == "" {
		cmd.Auth.TenantID = cmd.ResourceTenantID
	}

	res, err := h.service.Authorize(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}

	if res.Decision == contracts.AuthDecisionDeny {
		h.writeResponse(w, r, res.HTTPStatus, res, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, res, start)
}

func (h *AuthenticationAuthorizationHandler) RegisterAPIKey(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_register_api_key")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.RegisterAPIKeyCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}

	res, err := h.service.RegisterAPIKey(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusCreated, res, start)
}

func (h *AuthenticationAuthorizationHandler) RegisterServiceIdentity(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_register_service_identity")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.RegisterServiceIdentityCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}

	res, err := h.service.RegisterServiceIdentity(ctx, cmd)
	if err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusCreated, res, start)
}

func (h *AuthenticationAuthorizationHandler) RevokeCredential(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.StartSpan(r.Context(), "auth.api_revoke_credential")
	defer span.End()
	h.setStandardHeaders(w, r)
	start := time.Now()

	var cmd contracts.RevokeCredentialCommand
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		h.writeError(w, r, appErrors.NewValidationError(err.Error()), start)
		return
	}

	if cmd.TenantID == "" {
		cmd.TenantID = r.Header.Get(constants.HeaderXTenantID)
	}

	if err := h.service.RevokeCredential(ctx, cmd); err != nil {
		h.writeError(w, r, err, start)
		return
	}
	h.writeResponse(w, r, http.StatusOK, map[string]string{"status": "revoked"}, start)
}

func (h *AuthenticationAuthorizationHandler) setStandardHeaders(w http.ResponseWriter, r *http.Request) {
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

func (h *AuthenticationAuthorizationHandler) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, data any, start time.Time) {
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

func (h *AuthenticationAuthorizationHandler) writeError(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
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

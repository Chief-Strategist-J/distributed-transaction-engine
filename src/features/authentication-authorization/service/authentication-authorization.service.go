package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	audittrail "distributed-transaction-engine/src/features/audit-trail"
	auditContracts "distributed-transaction-engine/src/features/audit-trail/contracts"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/features/authentication-authorization/domain/machines"
	"distributed-transaction-engine/src/features/authentication-authorization/domain/rules"
	"distributed-transaction-engine/src/features/authentication-authorization/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo     ports.AuthRepositoryPort
	Verifier ports.TokenVerifierPort
	AuditSvc *audittrail.AuditTrailService
	Tracer   *observability.DefaultTracer
}

type AuthenticationAuthorizationService struct {
	repo     ports.AuthRepositoryPort
	verifier ports.TokenVerifierPort
	auditSvc *audittrail.AuditTrailService
	tracer   *observability.DefaultTracer
	machine  *machines.AuthRiskMachine
}

func NewAuthenticationAuthorizationService(deps ServiceDeps) *AuthenticationAuthorizationService {
	tracer := deps.Tracer
	if tracer == nil {
		tracer = observability.NewTracer("authentication-authorization-service")
	}
	return &AuthenticationAuthorizationService{
		repo:     deps.Repo,
		verifier: deps.Verifier,
		auditSvc: deps.AuditSvc,
		tracer:   tracer,
		machine:  machines.NewAuthRiskMachine(),
	}
}

func (s *AuthenticationAuthorizationService) AuthenticateToken(ctx context.Context, cmd contracts.AuthenticateTokenCommand) (spanResult0 *contracts.AuthContext, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_authenticate_token")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateAuthenticateToken(&inputCmd); err != nil {
		return nil, err
	}

	verified, err := s.verifyToken(ctx, inputCmd)
	if err != nil {
		return nil, err
	}

	revoked, err := s.repo.IsRevoked(ctx, verified.TenantID, contracts.RevocationTargetTypeToken, verified.TokenID)
	if err != nil {
		return nil, err
	}
	if revoked {
		return nil, appErrors.NewUnauthorizedError("token has been revoked")
	}

	return &contracts.AuthContext{
		CallerType:     contracts.CallerTypeUserOIDC,
		PrincipalID:    verified.Subject,
		TenantID:       verified.TenantID,
		Scopes:         verified.Scopes,
		Roles:          verified.Roles,
		StepUpVerified: false,
		TokenJTI:       verified.TokenID,
	}, nil
}

func (s *AuthenticationAuthorizationService) verifyToken(ctx context.Context, cmd contracts.AuthenticateTokenCommand) (spanResult0 *contracts.VerifiedToken, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if s.verifier == nil {
		return nil, appErrors.NewUnauthorizedError("token verification is not configured")
	}
	verified, err := s.verifier.Verify(ctx, cmd.Token)
	if err != nil {
		return nil, err
	}
	if verified.TokenID == "" {
		return nil, appErrors.NewUnauthorizedError("token id is required")
	}
	if cmd.Issuer != verified.Issuer || !containsString(verified.Audience, cmd.Audience) {
		return nil, appErrors.NewUnauthorizedError("token issuer or audience mismatch")
	}
	return verified, nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (s *AuthenticationAuthorizationService) AuthenticateAPIKey(ctx context.Context, cmd contracts.AuthenticateAPIKeyCommand) (spanResult0 *contracts.AuthContext, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_authenticate_api_key")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateAuthenticateAPIKey(&inputCmd); err != nil {
		return nil, err
	}

	lastIdx := strings.LastIndex(inputCmd.RawKey, "_")
	if lastIdx <= 0 || lastIdx >= len(inputCmd.RawKey)-1 {
		return nil, appErrors.NewUnauthorizedError("invalid api key format")
	}
	prefix := inputCmd.RawKey[:lastIdx]
	secret := inputCmd.RawKey[lastIdx+1:]

	keyRecord, err := s.repo.GetAPIKeyByPrefix(ctx, inputCmd.TenantID, prefix)
	if err != nil {
		return nil, err
	}
	if keyRecord == nil {
		return nil, appErrors.NewUnauthorizedError("invalid api key credentials")
	}

	if keyRecord.Status != contracts.APIKeyStatusActive {
		return nil, appErrors.NewUnauthorizedError("api key is not active")
	}

	if keyRecord.ExpiresAt != nil && keyRecord.ExpiresAt.Before(time.Now().UTC()) {
		return nil, appErrors.NewUnauthorizedError("api key has expired")
	}

	revoked, err := s.repo.IsRevoked(ctx, inputCmd.TenantID, contracts.RevocationTargetTypeKey, keyRecord.PublicID)
	if err != nil {
		return nil, err
	}
	if revoked {
		return nil, appErrors.NewUnauthorizedError("api key has been revoked")
	}

	if !rules.ValidateAPIKeySecret(secret, keyRecord.KeyHash) {
		return nil, appErrors.NewUnauthorizedError("invalid api key credentials")
	}

	return &contracts.AuthContext{
		CallerType:     contracts.CallerTypeAPIKey,
		PrincipalID:    keyRecord.PublicID,
		TenantID:       keyRecord.TenantID,
		Scopes:         keyRecord.Scopes,
		Roles:          []string{"api_client"},
		StepUpVerified: false,
	}, nil
}

func (s *AuthenticationAuthorizationService) AuthenticateService(ctx context.Context, cmd contracts.AuthenticateServiceCommand) (spanResult0 *contracts.AuthContext, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_authenticate_service")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateAuthenticateService(&inputCmd); err != nil {
		return nil, err
	}

	tenantID := "tenant-alpha"
	serviceIdentity, err := s.repo.GetServiceIdentity(ctx, tenantID, inputCmd.SPIFFEID)
	if err != nil {
		return nil, err
	}
	if serviceIdentity == nil {
		return nil, appErrors.NewForbiddenError("service identity not registered in mesh policy")
	}

	if !rules.ValidateSPIFFEMethodPolicy(inputCmd.SPIFFEID, inputCmd.Method, serviceIdentity.AllowedMethods) {
		return nil, appErrors.NewForbiddenError("service identity not authorized for method: " + inputCmd.Method)
	}

	authCtx := &contracts.AuthContext{
		CallerType:     contracts.CallerTypeInternalService,
		PrincipalID:    serviceIdentity.PublicID,
		TenantID:       tenantID,
		Scopes:         serviceIdentity.AllowedMethods,
		Roles:          []string{"internal_mesh_service"},
		SPIFFEID:       inputCmd.SPIFFEID,
		StepUpVerified: false,
	}

	if inputCmd.OnBehalfOfToken != "" {
		authCtx.DelegatedUser = "usr_delegated_owner"
	}

	return authCtx, nil
}

func (s *AuthenticationAuthorizationService) AuthenticateWebhook(ctx context.Context, cmd contracts.AuthenticateWebhookCommand) (spanResult0 *contracts.AuthContext, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_authenticate_webhook")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateAuthenticateWebhook(&inputCmd); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	if !rules.ValidateWebhookTimestampTolerance(inputCmd.Timestamp, now, 5*time.Minute) {
		return nil, appErrors.NewUnauthorizedError("webhook timestamp outside allowed skew tolerance")
	}

	hasReplay, err := s.repo.HasReplayNonce(ctx, inputCmd.TenantID, inputCmd.Provider, inputCmd.Nonce)
	if err != nil {
		return nil, err
	}
	if hasReplay {
		return nil, appErrors.NewConflictError("replay detected: webhook nonce already processed")
	}

	if !rules.ValidateWebhookSignature(inputCmd.ProviderSecret, inputCmd.RawPayload, inputCmd.Timestamp, inputCmd.Nonce, inputCmd.Signature) {
		return nil, appErrors.NewUnauthorizedError("invalid webhook provider signature")
	}

	replayRecord := &contracts.ProviderReplayRecord{
		ID:            uuid.New(),
		PublicID:      s.generatePublicID("prc"),
		TenantID:      inputCmd.TenantID,
		Source:        "WEBHOOK_RECEIVER",
		ExternalID:    inputCmd.Nonce,
		CorrelationID: uuid.New(),
		Provider:      inputCmd.Provider,
		Nonce:         inputCmd.Nonce,
		ReceivedAt:    now,
		ExpiresAt:     now.Add(15 * time.Minute),
	}
	observability.NoteSwallowed(s.repo.SaveReplayNonce(ctx, replayRecord))

	return &contracts.AuthContext{
		CallerType:     contracts.CallerTypeProviderWebhook,
		PrincipalID:    "provider:" + inputCmd.Provider,
		TenantID:       inputCmd.TenantID,
		Scopes:         []string{"webhook:callback"},
		Roles:          []string{"external_provider"},
		StepUpVerified: false,
	}, nil
}

func (s *AuthenticationAuthorizationService) Authorize(ctx context.Context, cmd contracts.AuthorizeRequestCommand) (spanResult0 *contracts.AuthorizeResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_authorize")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateAuthorizeRequest(&inputCmd); err != nil {
		return nil, err
	}

	decisionResult := rules.Evaluate5LayerAuthorization(inputCmd)
	auditID := "auth_dec_" + uuid.New().String()
	decisionResult.AuditID = auditID

	now := time.Now().UTC()
	decisionRecord := &contracts.AuthDecisionRecord{
		ID:             uuid.New(),
		PublicID:       s.generatePublicID("ada"),
		TenantID:       inputCmd.Auth.TenantID,
		Source:         "AUTH_ENGINE",
		ExternalID:     auditID,
		CorrelationID:  uuid.New(),
		CallerType:     inputCmd.Auth.CallerType,
		PrincipalID:    inputCmd.Auth.PrincipalID,
		Action:         inputCmd.Action,
		ResourceID:     inputCmd.ResourceID,
		Decision:       decisionResult.Decision,
		ReasonCode:     decisionResult.ReasonCode,
		RiskLevel:      s.resolveRiskLevel(inputCmd),
		StepUpVerified: inputCmd.Auth.StepUpVerified,
		ApproverID:     inputCmd.ApproverID,
		CreatedAt:      now,
	}
	observability.NoteSwallowed(s.repo.SaveAuthDecision(ctx, decisionRecord))

	if s.auditSvc != nil && (decisionResult.Decision == contracts.AuthDecisionDeny || inputCmd.ApproverID != "") {
		_, swallowedErr1 := s.auditSvc.CaptureOutboxEvent(ctx, auditContracts.CaptureOutboxEventCommand{
			TenantID:       inputCmd.Auth.TenantID,
			Source:         "AUTH_DECISION_ENGINE",
			ExternalID:     auditID,
			CorrelationID:  decisionRecord.CorrelationID,
			AuditID:        auditID,
			OccurredAt:     now,
			Action:         "AUTH_DECISION_" + string(decisionResult.Decision),
			ActorID:        inputCmd.Auth.PrincipalID,
			TargetResource: inputCmd.ResourceID,
			Payload: map[string]interface{}{
				"decision":    string(decisionResult.Decision),
				"reason_code": decisionResult.ReasonCode,
				"action":      inputCmd.Action,
				"approver_id": inputCmd.ApproverID,
			},
		})
		observability.NoteSwallowed(swallowedErr1)
	}

	return &decisionResult, nil
}

func (s *AuthenticationAuthorizationService) RegisterAPIKey(ctx context.Context, cmd contracts.RegisterAPIKeyCommand) (spanResult0 *contracts.RegisterAPIKeyResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_register_api_key")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateRegisterAPIKey(&inputCmd); err != nil {
		return nil, err
	}

	prefixBytes := make([]byte, 4)
	_, swallowedErr2 := rand.Read(prefixBytes)
	observability.NoteSwallowed(swallowedErr2)
	prefix := "apk_" + hex.EncodeToString(prefixBytes)

	secretBytes := make([]byte, 24)
	_, swallowedErr3 := rand.Read(secretBytes)
	observability.NoteSwallowed(swallowedErr3)
	secret := hex.EncodeToString(secretBytes)

	keyHash := rules.HashAPIKeySecret(secret)
	record := &contracts.APIKeyRecord{
		ID:            uuid.New(),
		PublicID:      s.generatePublicID("apk"),
		TenantID:      inputCmd.TenantID,
		Source:        "AUTH_ADMIN",
		ExternalID:    uuid.New().String(),
		CorrelationID: uuid.New(),
		KeyPrefix:     prefix,
		KeyHash:       keyHash,
		Name:          inputCmd.Name,
		Scopes:        inputCmd.Scopes,
		AllowedCIDRs:  inputCmd.AllowedCIDRs,
		Status:        contracts.APIKeyStatusActive,
		ExpiresAt:     inputCmd.ExpiresAt,
		CreatedAt:     time.Now().UTC(),
	}

	if err := s.repo.SaveAPIKey(ctx, record); err != nil {
		return nil, err
	}

	return &contracts.RegisterAPIKeyResult{
		PublicID:  record.PublicID,
		KeyPrefix: prefix,
		SecretKey: prefix + "_" + secret,
		ExpiresAt: record.ExpiresAt,
	}, nil
}

func (s *AuthenticationAuthorizationService) RegisterServiceIdentity(ctx context.Context, cmd contracts.RegisterServiceIdentityCommand) (spanResult0 *contracts.ServiceIdentityRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_register_service_identity")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateRegisterServiceIdentity(&inputCmd); err != nil {
		return nil, err
	}

	record := &contracts.ServiceIdentityRecord{
		ID:             uuid.New(),
		PublicID:       s.generatePublicID("svi"),
		TenantID:       inputCmd.TenantID,
		Source:         "PLATFORM_MESH",
		ExternalID:     uuid.New().String(),
		CorrelationID:  uuid.New(),
		SPIFFEID:       inputCmd.SPIFFEID,
		TrustDomain:    inputCmd.TrustDomain,
		AllowedMethods: inputCmd.AllowedMethods,
		Status:         "ACTIVE",
		CreatedAt:      time.Now().UTC(),
	}

	if err := s.repo.SaveServiceIdentity(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *AuthenticationAuthorizationService) RevokeCredential(ctx context.Context, cmd contracts.RevokeCredentialCommand) (spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "auth.service_revoke_credential")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateRevokeCredential(&inputCmd); err != nil {
		return err
	}

	record := &contracts.RevocationRecord{
		ID:               uuid.New(),
		PublicID:         s.generatePublicID("rev"),
		TenantID:         inputCmd.TenantID,
		Source:           "AUTH_ADMIN",
		ExternalID:       uuid.New().String(),
		CorrelationID:    uuid.New(),
		TargetType:       inputCmd.TargetType,
		TargetID:         inputCmd.TargetID,
		RevocationReason: inputCmd.RevocationReason,
		RevokedAt:        time.Now().UTC(),
		ExpiresAt:        inputCmd.ExpiresAt,
		CreatedAt:        time.Now().UTC(),
	}

	return s.repo.SaveRevocation(ctx, record)
}

func (s *AuthenticationAuthorizationService) resolveRiskLevel(cmd contracts.AuthorizeRequestCommand) contracts.RiskLevel {
	if cmd.Amount >= 50000.0 || strings.EqualFold(cmd.Action, "tx:force_complete") {
		return contracts.RiskLevelCritical
	}
	if cmd.Amount >= 10000.0 || strings.EqualFold(cmd.Action, "tx:redrive") {
		return contracts.RiskLevelHigh
	}
	if strings.EqualFold(cmd.Action, "tx:cancel") {
		return contracts.RiskLevelMedium
	}
	return contracts.RiskLevelLow
}

func (s *AuthenticationAuthorizationService) generatePublicID(prefix string) string {
	b := make([]byte, 13)
	_, swallowedErr4 := rand.Read(b)
	observability.NoteSwallowed(swallowedErr4)
	return prefix + "_" + hex.EncodeToString(b)
}

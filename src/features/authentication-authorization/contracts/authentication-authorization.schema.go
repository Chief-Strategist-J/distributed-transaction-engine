package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"strings"
)

func ValidateAuthenticateToken(cmd *AuthenticateTokenCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.Token) == "" {
		return appErrors.NewValidationError("token is required")
	}
	if strings.TrimSpace(cmd.Issuer) == "" {
		return appErrors.NewValidationError("issuer is required")
	}
	if strings.TrimSpace(cmd.Audience) == "" {
		return appErrors.NewValidationError("audience is required")
	}
	return nil
}

func ValidateAuthenticateAPIKey(cmd *AuthenticateAPIKeyCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.RawKey) == "" {
		return appErrors.NewValidationError("raw_key is required")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	return nil
}

func ValidateAuthenticateService(cmd *AuthenticateServiceCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.SPIFFEID) == "" {
		return appErrors.NewValidationError("spiffe_id is required")
	}
	if strings.TrimSpace(cmd.Method) == "" {
		return appErrors.NewValidationError("method is required")
	}
	return nil
}

func ValidateAuthenticateWebhook(cmd *AuthenticateWebhookCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.Provider) == "" {
		return appErrors.NewValidationError("provider is required")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(cmd.Signature) == "" {
		return appErrors.NewValidationError("signature is required")
	}
	if strings.TrimSpace(cmd.Nonce) == "" {
		return appErrors.NewValidationError("nonce is required")
	}
	if cmd.Timestamp.IsZero() {
		return appErrors.NewValidationError("timestamp is required")
	}
	return nil
}

func ValidateAuthorizeRequest(cmd *AuthorizeRequestCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.Auth.PrincipalID) == "" {
		return appErrors.NewValidationError("principal_id is required")
	}
	if strings.TrimSpace(cmd.Auth.TenantID) == "" {
		return appErrors.NewValidationError("caller tenant_id is required")
	}
	if strings.TrimSpace(cmd.ResourceTenantID) == "" {
		return appErrors.NewValidationError("resource_tenant_id is required")
	}
	if strings.TrimSpace(cmd.Action) == "" {
		return appErrors.NewValidationError("action is required")
	}
	return nil
}

func ValidateRegisterAPIKey(cmd *RegisterAPIKeyCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(cmd.Name) == "" {
		return appErrors.NewValidationError("name is required")
	}
	return nil
}

func ValidateRegisterServiceIdentity(cmd *RegisterServiceIdentityCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(cmd.SPIFFEID) == "" {
		return appErrors.NewValidationError("spiffe_id is required")
	}
	if len(cmd.AllowedMethods) == 0 {
		return appErrors.NewValidationError("allowed_methods cannot be empty")
	}
	return nil
}

func ValidateRevokeCredential(cmd *RevokeCredentialCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(cmd.TargetID) == "" {
		return appErrors.NewValidationError("target_id is required")
	}
	if cmd.ExpiresAt.IsZero() {
		return appErrors.NewValidationError("expires_at is required")
	}
	return nil
}

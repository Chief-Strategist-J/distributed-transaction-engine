package adapters

import (
	"context"
	stderrors "errors"

	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/infra/auth"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type PrincipalVerifier interface {
	Verify(token string) (auth.Principal, error)
}

type JWTTokenVerifierAdapter struct {
	verifier PrincipalVerifier
}

func NewJWTTokenVerifierAdapter(verifier PrincipalVerifier) *JWTTokenVerifierAdapter {
	return &JWTTokenVerifierAdapter{verifier: verifier}
}

func (a *JWTTokenVerifierAdapter) Verify(ctx context.Context, token string) (spanResult0 *contracts.VerifiedToken, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	principal, err := a.verifier.Verify(token)
	if err != nil {
		if stderrors.Is(err, auth.ErrInvalidToken) {
			return nil, appErrors.NewUnauthorizedError("invalid or expired token")
		}
		return nil, appErrors.NewInternalError("token verification failed")
	}
	return &contracts.VerifiedToken{
		Subject: principal.Subject, TenantID: principal.TenantID, Issuer: principal.Issuer, Audience: principal.Audience,
		TokenID: principal.TokenID, Roles: principal.Roles, Scopes: principal.Scopes, ExpiresAt: principal.ExpiresAt,
	}, nil
}

package chassis

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"distributed-transaction-engine/src/infra/auth"
	"distributed-transaction-engine/src/infra/config"
	"distributed-transaction-engine/src/shared/constants"
)

type AuthWrapper func(http.Handler) http.Handler

func BuildVerifier(cfg config.AuthConfig, environment string, clock func() time.Time) (*auth.Verifier, error) {
	if cfg.ResolvedMode(environment) == constants.AuthModeDisabled {
		return nil, nil
	}
	jwks, err := os.ReadFile(cfg.JWKSFile)
	if err != nil {
		return nil, fmt.Errorf("read jwks file: %w", err)
	}
	verifier, err := auth.NewVerifier(auth.Config{Issuer: cfg.Issuer, Audience: cfg.Audience, Leeway: cfg.Leeway()}, jwks, clock)
	if err != nil {
		return nil, fmt.Errorf("build jwt verifier: %w", err)
	}
	return verifier, nil
}

func WrapperFor(verifier *auth.Verifier) AuthWrapper {
	if verifier == nil {
		log.Printf("WARNING: authentication is disabled; tenant and operator identity are taken from request headers\n")
		return auth.TrustedHeaders
	}
	return func(next http.Handler) http.Handler { return auth.Middleware(verifier, next) }
}

func BuildAuthWrapper(cfg config.AuthConfig, environment string, clock func() time.Time) (AuthWrapper, error) {
	verifier, err := BuildVerifier(cfg, environment, clock)
	if err != nil {
		return nil, err
	}
	return WrapperFor(verifier), nil
}

func (a *App) wrapAuth(next http.Handler) http.Handler {
	if a.AuthWrap == nil {
		return next
	}
	return a.AuthWrap(next)
}

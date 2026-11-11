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

func BuildAuthWrapper(cfg config.AuthConfig, environment string, clock func() time.Time) (AuthWrapper, error) {
	if cfg.ResolvedMode(environment) == constants.AuthModeDisabled {
		log.Printf("WARNING: authentication is disabled; tenant and operator identity are taken from request headers\n")
		return func(next http.Handler) http.Handler { return next }, nil
	}
	jwks, err := os.ReadFile(cfg.JWKSFile)
	if err != nil {
		return nil, fmt.Errorf("read jwks file: %w", err)
	}
	verifier, err := auth.NewVerifier(auth.Config{Issuer: cfg.Issuer, Audience: cfg.Audience, Leeway: cfg.Leeway()}, jwks, clock)
	if err != nil {
		return nil, fmt.Errorf("build jwt verifier: %w", err)
	}
	return func(next http.Handler) http.Handler { return auth.Middleware(verifier, next) }, nil
}

func (a *App) wrapAuth(next http.Handler) http.Handler {
	if a.AuthWrap == nil {
		return next
	}
	return a.AuthWrap(next)
}

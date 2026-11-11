package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"distributed-transaction-engine/src/shared/constants"
)

var secureEnvironments = map[string]bool{"production": true, "staging": true}

func (a *AuthConfig) ApplyEnvironment() {
	a.Mode = firstSet(os.Getenv("AUTH_MODE"), a.Mode)
	a.JWKSFile = firstSet(os.Getenv("AUTH_JWKS_FILE"), a.JWKSFile)
	a.Issuer = firstSet(os.Getenv("AUTH_ISSUER"), a.Issuer)
	a.Audience = firstSet(os.Getenv("AUTH_AUDIENCE"), a.Audience)
	if leeway, err := strconv.Atoi(os.Getenv("AUTH_LEEWAY_SECONDS")); err == nil && leeway >= 0 {
		a.LeewaySeconds = leeway
	}
}

func (a *AuthConfig) ResolvedMode(environment string) string {
	mode := strings.ToLower(strings.TrimSpace(a.Mode))
	if mode != "" {
		return mode
	}
	if secureEnvironments[strings.ToLower(strings.TrimSpace(environment))] {
		return constants.AuthModeJWT
	}
	return constants.AuthModeDisabled
}

func (a *AuthConfig) Validate(environment string) error {
	mode := a.ResolvedMode(environment)
	switch mode {
	case constants.AuthModeDisabled:
		if secureEnvironments[strings.ToLower(strings.TrimSpace(environment))] {
			return errors.New("authentication cannot be disabled in production or staging")
		}
		return nil
	case constants.AuthModeJWT:
		return a.validateJWT()
	default:
		return fmt.Errorf("unsupported auth mode %q", a.Mode)
	}
}

func (a *AuthConfig) validateJWT() error {
	if a.JWKSFile == "" || a.Issuer == "" || a.Audience == "" {
		return errors.New("jwt auth requires AUTH_JWKS_FILE, AUTH_ISSUER and AUTH_AUDIENCE")
	}
	if a.LeewaySeconds < 0 || a.LeewaySeconds > 300 {
		return errors.New("AUTH_LEEWAY_SECONDS must be between 0 and 300")
	}
	return nil
}

func (a *AuthConfig) Leeway() time.Duration {
	return time.Duration(a.LeewaySeconds) * time.Second
}

func firstSet(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

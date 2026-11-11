package auth

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var ErrInvalidToken = errors.New("invalid or expired token")

type Principal struct {
	Subject  string
	TenantID string
	Roles    []string
}

type Config struct {
	Issuer   string
	Audience string
	Leeway   time.Duration
}

type claims struct {
	jwt.RegisteredClaims
	TenantID string   `json:"tenant_id"`
	Roles    []string `json:"roles"`
}

type Verifier struct {
	keys   map[string]crypto.PublicKey
	parser *jwt.Parser
}

type principalKey struct{}

var allowedMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}

func NewVerifier(cfg Config, jwks []byte, clock func() time.Time) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("issuer and audience are required")
	}
	keys, err := ParseJWKS(jwks)
	if err != nil {
		return nil, err
	}
	parser := jwt.NewParser(
		jwt.WithValidMethods(allowedMethods),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithAudience(cfg.Audience),
		jwt.WithLeeway(cfg.Leeway),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(clock),
	)
	return &Verifier{keys: keys, parser: parser}, nil
}

func (v *Verifier) Verify(token string) (Principal, error) {
	parsed := &claims{}
	if _, err := v.parser.ParseWithClaims(token, parsed, v.keyFor); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if parsed.Subject == "" || strings.TrimSpace(parsed.TenantID) == "" {
		return Principal{}, fmt.Errorf("%w: sub and tenant_id claims are required", ErrInvalidToken)
	}
	return Principal{Subject: parsed.Subject, TenantID: parsed.TenantID, Roles: parsed.Roles}, nil
}

func (v *Verifier) keyFor(token *jwt.Token) (interface{}, error) {
	kid, _ := token.Header["kid"].(string)
	key, found := v.keys[kid]
	if kid == "" || !found {
		return nil, errors.New("unknown signing key")
	}
	return key, nil
}

func (p Principal) HasRole(role string) bool {
	for _, candidate := range p.Roles {
		if candidate == role {
			return true
		}
	}
	return false
}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalKey{}).(Principal)
	return principal, ok
}

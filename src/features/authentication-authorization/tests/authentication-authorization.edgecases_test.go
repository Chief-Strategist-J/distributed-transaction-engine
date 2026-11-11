package tests

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	auth "distributed-transaction-engine/src/features/authentication-authorization"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	infraauth "distributed-transaction-engine/src/infra/auth"
	appErrors "distributed-transaction-engine/src/shared/errors"
	stderrors "errors"
	"github.com/golang-jwt/jwt/v5"
)

const (
	edgeIssuer   = "https://issuer.example.test"
	edgeAudience = "dtx-api"
)

type tokenFixture struct {
	key *rsa.PrivateKey
	svc *auth.AuthenticationAuthorizationService
}

func newTokenFixture(t *testing.T) tokenFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encode := func(value *big.Int) string { return base64.RawURLEncoding.EncodeToString(value.Bytes()) }
	jwks, _ := json.Marshal(map[string]any{"keys": []map[string]string{
		{"kty": "RSA", "kid": "k1", "use": "sig", "n": encode(key.N), "e": encode(big.NewInt(int64(key.E)))},
	}})
	verifier, err := infraauth.NewVerifier(infraauth.Config{Issuer: edgeIssuer, Audience: edgeAudience}, jwks, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	svc := auth.NewService(auth.ServiceDeps{Repo: auth.NewMemoryRepository(), Verifier: auth.NewJWTTokenVerifier(verifier)})
	return tokenFixture{key: key, svc: svc}
}

func (f tokenFixture) token(t *testing.T, mutate func(jwt.MapClaims)) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss": edgeIssuer, "aud": edgeAudience, "sub": "usr_77", "tenant_id": "tenant_zeta", "jti": "jti-1",
		"roles": []string{"standard_user"}, "scope": "tx:view tx:start", "scp": []string{"tx:view", "tx:cancel"},
		"exp": time.Now().Add(time.Minute).Unix(),
	}
	if mutate != nil {
		mutate(claims)
	}
	signed := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed.Header["kid"] = "k1"
	out, err := signed.SignedString(f.key)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func command(token string) contracts.AuthenticateTokenCommand {
	return contracts.AuthenticateTokenCommand{Token: token, Issuer: edgeIssuer, Audience: edgeAudience}
}

func requireStatus(t *testing.T, err error, status int) {
	t.Helper()
	var appErr *appErrors.ApplicationError
	if !stderrors.As(err, &appErr) || appErr.StatusCode != status {
		t.Fatalf("expected status %d, got %#v", status, err)
	}
}

func TestEdgeCases_AuthenticateTokenReturnsVerifiedClaims(t *testing.T) {
	f := newTokenFixture(t)
	authCtx, err := f.svc.AuthenticateToken(context.Background(), command(f.token(t, nil)))
	if err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	if authCtx.PrincipalID != "usr_77" || authCtx.TenantID != "tenant_zeta" || authCtx.TokenJTI != "jti-1" {
		t.Fatalf("identity must come from the token, got %#v", authCtx)
	}
	if len(authCtx.Scopes) != 3 || authCtx.Roles[0] != "standard_user" || authCtx.StepUpVerified {
		t.Fatalf("scopes and roles must come from the token: %#v", authCtx)
	}
}

func TestEdgeCases_AuthenticateTokenRejectsInvalidTokens(t *testing.T) {
	f := newTokenFixture(t)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	forged := tokenFixture{key: other, svc: f.svc}
	cases := map[string]string{
		"expired":        f.token(t, func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Hour).Unix() }),
		"wrong_issuer":   f.token(t, func(c jwt.MapClaims) { c["iss"] = "https://evil.test" }),
		"wrong_audience": f.token(t, func(c jwt.MapClaims) { c["aud"] = "other" }),
		"missing_tenant": f.token(t, func(c jwt.MapClaims) { delete(c, "tenant_id") }),
		"missing_jti":    f.token(t, func(c jwt.MapClaims) { delete(c, "jti") }),
		"forged_key":     forged.token(t, nil),
		"three_dots":     "a.b.c",
		"unsigned_shape": "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ4In0.",
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := f.svc.AuthenticateToken(context.Background(), command(token))
			requireStatus(t, err, http.StatusUnauthorized)
		})
	}
}

func TestEdgeCases_AuthenticateTokenIgnoresCallerSuppliedIssuerAndAudience(t *testing.T) {
	f := newTokenFixture(t)
	token := f.token(t, nil)
	for _, cmd := range []contracts.AuthenticateTokenCommand{
		{Token: token, Issuer: "https://evil.test", Audience: edgeAudience},
		{Token: token, Issuer: edgeIssuer, Audience: "other"},
	} {
		_, err := f.svc.AuthenticateToken(context.Background(), cmd)
		requireStatus(t, err, http.StatusUnauthorized)
	}
}

func TestEdgeCases_AuthenticateTokenHonoursRevocation(t *testing.T) {
	f := newTokenFixture(t)
	ctx := context.Background()
	token := f.token(t, nil)
	if _, err := f.svc.AuthenticateToken(ctx, command(token)); err != nil {
		t.Fatalf("token must work before revocation: %v", err)
	}
	revoke := contracts.RevokeCredentialCommand{
		TenantID: "tenant_zeta", TargetType: contracts.RevocationTargetTypeToken, TargetID: "jti-1",
		RevocationReason: "compromised", ExpiresAt: time.Now().Add(time.Hour),
	}
	if err := f.svc.RevokeCredential(ctx, revoke); err != nil {
		t.Fatal(err)
	}
	_, err := f.svc.AuthenticateToken(ctx, command(token))
	requireStatus(t, err, http.StatusUnauthorized)
	otherTenant := f.token(t, func(c jwt.MapClaims) { c["tenant_id"] = "tenant_other" })
	if _, err = f.svc.AuthenticateToken(ctx, command(otherTenant)); err != nil {
		t.Fatalf("revocation must be scoped to its tenant: %v", err)
	}
}

func TestEdgeCases_AuthenticateTokenFailsClosedWithoutVerifier(t *testing.T) {
	svc := auth.NewService(auth.ServiceDeps{Repo: auth.NewMemoryRepository()})
	_, err := svc.AuthenticateToken(context.Background(), command("a.b.c"))
	requireStatus(t, err, http.StatusUnauthorized)
}

func newKeyService() (*auth.AuthenticationAuthorizationService, auth.AuthRepositoryPort) {
	repo := auth.NewMemoryRepository()
	return auth.NewService(auth.ServiceDeps{Repo: repo}), repo
}

func TestEdgeCases_APIKeysAreStoredOnlyAsHashesAndShownOnce(t *testing.T) {
	svc, repo := newKeyService()
	ctx := context.Background()
	registered, err := svc.RegisterAPIKey(ctx, contracts.RegisterAPIKeyCommand{TenantID: "tenant_keys", Name: "ci", Scopes: []string{"tx:view"}})
	if err != nil {
		t.Fatal(err)
	}
	secret := strings.TrimPrefix(registered.SecretKey, registered.KeyPrefix+"_")
	if len(secret) != 48 {
		t.Fatalf("the secret must carry 192 bits of entropy, got %d hex characters", len(secret))
	}
	stored, err := repo.GetAPIKeyByPrefix(ctx, "tenant_keys", registered.KeyPrefix)
	if err != nil || stored == nil {
		t.Fatalf("stored key missing: %v", err)
	}
	encoded, _ := json.Marshal(stored)
	if strings.Contains(string(encoded), secret) || stored.KeyHash == secret || len(stored.KeyHash) != 64 {
		t.Fatalf("the plaintext secret must never be stored: %s", encoded)
	}
	if _, err = svc.AuthenticateAPIKey(ctx, contracts.AuthenticateAPIKeyCommand{RawKey: registered.SecretKey, TenantID: "tenant_keys"}); err != nil {
		t.Fatalf("a freshly issued key must authenticate: %v", err)
	}
}

func TestEdgeCases_APIKeyAuthenticationRejectsWrongSecretsAndOtherTenants(t *testing.T) {
	svc, _ := newKeyService()
	ctx := context.Background()
	registered, _ := svc.RegisterAPIKey(ctx, contracts.RegisterAPIKeyCommand{TenantID: "tenant_keys_a", Name: "ci", Scopes: []string{"tx:view"}})
	tampered := registered.SecretKey[:len(registered.SecretKey)-1] + "0"
	if registered.SecretKey[len(registered.SecretKey)-1] == '0' {
		tampered = registered.SecretKey[:len(registered.SecretKey)-1] + "1"
	}
	cases := map[string]contracts.AuthenticateAPIKeyCommand{
		"wrong_secret": {RawKey: tampered, TenantID: "tenant_keys_a"},
		"other_tenant": {RawKey: registered.SecretKey, TenantID: "tenant_keys_b"},
		"prefix_only":  {RawKey: registered.KeyPrefix + "_", TenantID: "tenant_keys_a"},
		"garbage":      {RawKey: "not-a-key", TenantID: "tenant_keys_a"},
	}
	for name, cmd := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.AuthenticateAPIKey(ctx, cmd); err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		})
	}
}

func TestEdgeCases_EveryIssuedAPIKeyIsUnique(t *testing.T) {
	svc, _ := newKeyService()
	seen := map[string]bool{}
	for index := 0; index < 200; index++ {
		registered, err := svc.RegisterAPIKey(context.Background(), contracts.RegisterAPIKeyCommand{TenantID: "tenant_unique", Name: "k", Scopes: []string{"tx:view"}})
		if err != nil {
			continue
		}
		if seen[registered.SecretKey] {
			t.Fatal("duplicate api key issued")
		}
		seen[registered.SecretKey] = true
	}
	if len(seen) < 150 {
		t.Fatalf("expected the vast majority of registrations to succeed, got %d", len(seen))
	}
}

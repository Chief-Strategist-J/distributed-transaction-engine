package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testIssuer   = "https://issuer.example.test"
	testAudience = "dtx-api"
	testTenant   = "tenant_acme"
)

type fixture struct {
	rsaKey   *rsa.PrivateKey
	ecKey    *ecdsa.PrivateKey
	verifier *Verifier
	now      time.Time
}

func b64(value *big.Int) string { return base64.RawURLEncoding.EncodeToString(value.Bytes()) }

func buildJWKS(rsaKey *rsa.PrivateKey, ecKey *ecdsa.PrivateKey) []byte {
	document := map[string]any{"keys": []map[string]string{
		{"kty": "RSA", "kid": "rsa-1", "use": "sig", "n": b64(rsaKey.N), "e": b64(big.NewInt(int64(rsaKey.E)))},
		{"kty": "EC", "kid": "ec-1", "use": "sig", "crv": "P-256", "x": b64(ecKey.X), "y": b64(ecKey.Y)},
	}}
	encoded, _ := json.Marshal(document)
	return encoded
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	verifier, err := NewVerifier(Config{Issuer: testIssuer, Audience: testAudience, Leeway: 5 * time.Second}, buildJWKS(rsaKey, ecKey), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return fixture{rsaKey: rsaKey, ecKey: ecKey, verifier: verifier, now: now}
}

func (f fixture) claims(mutate func(jwt.MapClaims)) jwt.MapClaims {
	claims := jwt.MapClaims{
		"iss": testIssuer, "aud": testAudience, "sub": "usr_42", "tenant_id": testTenant,
		"roles": []string{"standard_user"}, "exp": f.now.Add(10 * time.Minute).Unix(), "nbf": f.now.Add(-time.Minute).Unix(),
	}
	if mutate != nil {
		mutate(claims)
	}
	return claims
}

func sign(t *testing.T, method jwt.SigningMethod, kid string, key any, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(method, claims)
	if kid != "" {
		token.Header["kid"] = kid
	}
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func (f fixture) validRSA(t *testing.T, mutate func(jwt.MapClaims)) string {
	return sign(t, jwt.SigningMethodRS256, "rsa-1", f.rsaKey, f.claims(mutate))
}

func publicKeyPEM(t *testing.T, key crypto.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func TestVerifier_AcceptsValidTokens(t *testing.T) {
	f := newFixture(t)
	cases := map[string]string{
		"rs256":                 f.validRSA(t, nil),
		"es256":                 sign(t, jwt.SigningMethodES256, "ec-1", f.ecKey, f.claims(nil)),
		"aud_list":              f.validRSA(t, func(c jwt.MapClaims) { c["aud"] = []string{"other", testAudience} }),
		"within_leeway_expired": f.validRSA(t, func(c jwt.MapClaims) { c["exp"] = f.now.Add(-3 * time.Second).Unix() }),
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			principal, err := f.verifier.Verify(token)
			if err != nil || principal.Subject != "usr_42" || principal.TenantID != testTenant {
				t.Fatalf("expected valid principal, got %#v err=%v", principal, err)
			}
		})
	}
}

func TestVerifier_RejectsAttackTokens(t *testing.T) {
	f := newFixture(t)
	otherKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	valid := f.validRSA(t, nil)
	parts := strings.Split(valid, ".")
	forgedPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"` + testIssuer + `","aud":"` + testAudience + `","sub":"evil","tenant_id":"tenant_victim","exp":` + "9999999999" + `}`))
	cases := map[string]string{
		"expired":                         f.validRSA(t, func(c jwt.MapClaims) { c["exp"] = f.now.Add(-time.Minute).Unix() }),
		"not_yet_valid":                   f.validRSA(t, func(c jwt.MapClaims) { c["nbf"] = f.now.Add(time.Hour).Unix() }),
		"missing_exp":                     f.validRSA(t, func(c jwt.MapClaims) { delete(c, "exp") }),
		"wrong_issuer":                    f.validRSA(t, func(c jwt.MapClaims) { c["iss"] = "https://evil.test" }),
		"wrong_audience":                  f.validRSA(t, func(c jwt.MapClaims) { c["aud"] = "someone-else" }),
		"missing_subject":                 f.validRSA(t, func(c jwt.MapClaims) { delete(c, "sub") }),
		"missing_tenant":                  f.validRSA(t, func(c jwt.MapClaims) { delete(c, "tenant_id") }),
		"blank_tenant":                    f.validRSA(t, func(c jwt.MapClaims) { c["tenant_id"] = "   " }),
		"unknown_kid":                     sign(t, jwt.SigningMethodRS256, "rsa-404", f.rsaKey, f.claims(nil)),
		"missing_kid":                     sign(t, jwt.SigningMethodRS256, "", f.rsaKey, f.claims(nil)),
		"signed_by_other_key":             sign(t, jwt.SigningMethodRS256, "rsa-1", otherKey, f.claims(nil)),
		"alg_none":                        sign(t, jwt.SigningMethodNone, "rsa-1", jwt.UnsafeAllowNoneSignatureType, f.claims(nil)),
		"hs256_with_public_key_as_secret": sign(t, jwt.SigningMethodHS256, "rsa-1", publicKeyPEM(t, &f.rsaKey.PublicKey), f.claims(nil)),
		"rs256_header_ec_key":             sign(t, jwt.SigningMethodRS256, "ec-1", f.rsaKey, f.claims(nil)),
		"tampered_payload":                parts[0] + "." + forgedPayload + "." + parts[2],
		"stripped_signature":              parts[0] + "." + parts[1] + ".",
		"garbage":                         "not-a-jwt",
		"empty":                           "",
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := f.verifier.Verify(token); err == nil {
				t.Fatalf("token %q must be rejected", name)
			}
		})
	}
}

func TestParseJWKS_RejectsUnsafeKeySets(t *testing.T) {
	small, _ := rsa.GenerateKey(rand.Reader, 1024)
	f := newFixture(t)
	good := buildJWKS(f.rsaKey, f.ecKey)
	cases := map[string]string{
		"not_json":      "{",
		"empty_set":     `{"keys":[]}`,
		"missing_kid":   `{"keys":[{"kty":"RSA","n":"AQAB","e":"AQAB"}]}`,
		"unsupported":   `{"keys":[{"kty":"oct","kid":"k","k":"c2VjcmV0"}]}`,
		"short_rsa":     `{"keys":[{"kty":"RSA","kid":"s","n":"` + b64(small.N) + `","e":"AQAB"}]}`,
		"off_curve":     `{"keys":[{"kty":"EC","kid":"e","crv":"P-256","x":"AQ","y":"AQ"}]}`,
		"unknown_curve": `{"keys":[{"kty":"EC","kid":"e","crv":"secp256k1","x":"AQ","y":"AQ"}]}`,
		"duplicate_kid": strings.Replace(string(good), `"ec-1"`, `"rsa-1"`, 1),
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseJWKS([]byte(document)); err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		})
	}
	if _, err := ParseJWKS(good); err != nil {
		t.Fatalf("good key set rejected: %v", err)
	}
}

func TestNewVerifier_RequiresIssuerAndAudience(t *testing.T) {
	f := newFixture(t)
	for _, cfg := range []Config{{}, {Issuer: testIssuer}, {Audience: testAudience}} {
		if _, err := NewVerifier(cfg, buildJWKS(f.rsaKey, f.ecKey), time.Now); err == nil {
			t.Fatalf("config %#v must be rejected", cfg)
		}
	}
}

type captured struct {
	called    bool
	header    http.Header
	body      string
	principal Principal
}

func (f fixture) serve(t *testing.T, request *http.Request) (*httptest.ResponseRecorder, *captured) {
	t.Helper()
	seen := &captured{}
	handler := Middleware(f.verifier, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.called, seen.header = true, r.Header.Clone()
		payload := make([]byte, 4096)
		count, _ := r.Body.Read(payload)
		seen.body = string(payload[:count])
		seen.principal, _ = PrincipalFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder, seen
}

func (f fixture) request(t *testing.T, method string, target string, body string, token string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return request
}

func TestMiddleware_RejectsUnauthenticatedRequests(t *testing.T) {
	f := newFixture(t)
	valid := f.validRSA(t, nil)
	cases := map[string]*http.Request{
		"no_header": f.request(t, http.MethodGet, "/api/v1/sagas", "", ""),
		"basic_scheme": func() *http.Request {
			r := f.request(t, http.MethodGet, "/x", "", "")
			r.Header.Set("Authorization", "Basic abc")
			return r
		}(),
		"bearer_empty": func() *http.Request {
			r := f.request(t, http.MethodGet, "/x", "", "")
			r.Header.Set("Authorization", "Bearer ")
			return r
		}(),
		"two_tokens": func() *http.Request {
			r := f.request(t, http.MethodGet, "/x", "", "")
			r.Header.Set("Authorization", "Bearer a b")
			return r
		}(),
		"expired": f.request(t, http.MethodGet, "/x", "", f.validRSA(t, func(c jwt.MapClaims) { c["exp"] = f.now.Add(-time.Hour).Unix() })),
		"tenant_header_only": func() *http.Request {
			r := f.request(t, http.MethodGet, "/x", "", "")
			r.Header.Set("x-tenant-id", testTenant)
			return r
		}(),
	}
	_ = valid
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			recorder, seen := f.serve(t, request)
			if recorder.Code != http.StatusUnauthorized || seen.called {
				t.Fatalf("expected 401 without reaching handler, got %d called=%v", recorder.Code, seen.called)
			}
			if recorder.Header().Get("WWW-Authenticate") != "Bearer" || strings.Contains(recorder.Body.String(), "signature") {
				t.Fatalf("bad challenge or leaked reason: %v %s", recorder.Header(), recorder.Body.String())
			}
		})
	}
}

func TestMiddleware_BindsVerifiedIdentityAndStripsSpoofedHeaders(t *testing.T) {
	f := newFixture(t)
	token := f.validRSA(t, func(c jwt.MapClaims) { c["roles"] = []string{"standard_user"} })
	request := f.request(t, http.MethodGet, "/api/v1/sagas", "", token)
	request.Header.Set("X-Operator-Role", "sre_admin")
	request.Header.Set("X-Principal-ID", "someone_else")
	recorder, seen := f.serve(t, request)
	if recorder.Code != http.StatusOK || !seen.called {
		t.Fatalf("expected pass-through, got %d", recorder.Code)
	}
	if seen.header.Get("x-tenant-id") != testTenant || seen.header.Get("X-Principal-ID") != "usr_42" {
		t.Fatalf("identity headers must come from the token: %v", seen.header)
	}
	if seen.header.Get("X-Operator-Role") != "" {
		t.Fatalf("spoofed operator role must be dropped, got %q", seen.header.Get("X-Operator-Role"))
	}
}

func TestMiddleware_GrantsOperatorRoleOnlyFromVerifiedClaims(t *testing.T) {
	f := newFixture(t)
	token := f.validRSA(t, func(c jwt.MapClaims) { c["roles"] = []string{"sre_admin"} })
	_, seen := f.serve(t, f.request(t, http.MethodGet, "/x", "", token))
	if seen.header.Get("X-Operator-Role") != "sre_admin" || !seen.principal.HasRole("sre_admin") {
		t.Fatalf("verified sre_admin role must reach the handler: %v", seen.header)
	}
}

func TestMiddleware_RejectsCrossTenantAttempts(t *testing.T) {
	f := newFixture(t)
	token := f.validRSA(t, nil)
	header := f.request(t, http.MethodGet, "/x", "", token)
	header.Header.Set("x-tenant-id", "tenant_victim")
	cases := map[string]*http.Request{
		"header":             header,
		"query_tenantId":     f.request(t, http.MethodGet, "/x?tenantId=tenant_victim", "", token),
		"query_tenant_id":    f.request(t, http.MethodGet, "/x?tenant_id=tenant_victim", "", token),
		"query_second_value": f.request(t, http.MethodGet, "/x?tenantId="+testTenant+"&tenantId=tenant_victim", "", token),
		"path":               f.request(t, http.MethodGet, "/v1/tenants/tenant_victim/workflows", "", token),
		"body_tenantId":      f.request(t, http.MethodPost, "/x", `{"tenantId":"tenant_victim"}`, token),
		"body_tenant_id":     f.request(t, http.MethodPost, "/x", `{"tenant_id":"tenant_victim"}`, token),
		"body_non_string":    f.request(t, http.MethodPost, "/x", `{"tenantId":123}`, token),
		"body_empty_string":  f.request(t, http.MethodPost, "/x", `{"tenantId":""}`, token),
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			recorder, seen := f.serve(t, request)
			if recorder.Code != http.StatusForbidden || seen.called || !strings.Contains(recorder.Body.String(), "TENANT_MISMATCH") {
				t.Fatalf("expected 403 TENANT_MISMATCH, got %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestMiddleware_AllowsOwnTenantAndPreservesBody(t *testing.T) {
	f := newFixture(t)
	token := f.validRSA(t, nil)
	body := `{"tenantId":"` + testTenant + `","name":"flow"}`
	cases := map[string]*http.Request{
		"body_matching":  f.request(t, http.MethodPost, "/x", body, token),
		"path_matching":  f.request(t, http.MethodGet, "/v1/tenants/"+testTenant+"/workflows", "", token),
		"query_matching": f.request(t, http.MethodGet, "/x?tenantId="+testTenant, "", token),
		"malformed_json": f.request(t, http.MethodPost, "/x", `{not json`, token),
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			recorder, seen := f.serve(t, request)
			if recorder.Code != http.StatusOK || !seen.called {
				t.Fatalf("expected pass-through, got %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
	_, seen := f.serve(t, f.request(t, http.MethodPost, "/x", body, token))
	if seen.body != body {
		t.Fatalf("body must be restored for the handler, got %q", seen.body)
	}
}

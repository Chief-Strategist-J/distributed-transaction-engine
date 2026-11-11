package chassis

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"distributed-transaction-engine/src/infra/config"
	"github.com/golang-jwt/jwt/v5"
)

func echoHandler(status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("response is not json: %q", recorder.Body.String())
	}
	return decoded
}

func TestRequestSizeLimitRejectsOversizedBodies(t *testing.T) {
	reached := false
	guarded := RequestSizeLimit(16, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))

	declared := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 64)))
	guarded.ServeHTTP(declared, request)
	if declared.Code != http.StatusRequestEntityTooLarge || reached {
		t.Fatalf("a declared oversized body must be refused with 413 before the handler, got %d reached=%v", declared.Code, reached)
	}

	chunked := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 64)))
	request.ContentLength = -1
	guarded.ServeHTTP(chunked, request)
	if chunked.Code != http.StatusRequestEntityTooLarge || reached {
		t.Fatalf("a chunked oversized body must also be refused, got %d reached=%v", chunked.Code, reached)
	}
	if decodeBody(t, chunked)["error"].(map[string]any)["code"] != "PAYLOAD_TOO_LARGE" {
		t.Fatal("413 must carry the PAYLOAD_TOO_LARGE error code")
	}
}

func TestRequestSizeLimitAllowsBodiesAtTheLimit(t *testing.T) {
	var received string
	guarded := RequestSizeLimit(16, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buffer := make([]byte, 32)
		count, _ := r.Body.Read(buffer)
		received = string(buffer[:count])
		w.WriteHeader(http.StatusOK)
	}))
	recorder := httptest.NewRecorder()
	guarded.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("0123456789abcdef")))
	if recorder.Code != http.StatusOK || received != "0123456789abcdef" {
		t.Fatalf("a body exactly at the limit must reach the handler intact, got %d %q", recorder.Code, received)
	}
}

func TestErrorSanitizerHidesDatabaseInternals(t *testing.T) {
	leaks := []string{
		`pq: duplicate key value violates unique constraint "idx_saga_ext"`,
		`pq: column "saga_id" of relation "inbox_messages" does not exist`,
		`ERROR: syntax error at or near "FROM" (SQLSTATE 42601)`,
		`insert into outbox_events values (1)`,
	}
	for _, leak := range leaks {
		body := `{"success":false,"statusCode":409,"error":{"code":"CONFLICT","message":` + mustQuote(leak) + `,"retryable":false}}`
		recorder := httptest.NewRecorder()
		ErrorSanitizer(false, echoHandler(http.StatusConflict, body)).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/x", nil))
		if recorder.Code != http.StatusConflict {
			t.Fatalf("the status code must be preserved, got %d", recorder.Code)
		}
		errorObject := decodeBody(t, recorder)["error"].(map[string]any)
		if strings.Contains(recorder.Body.String(), "constraint") || strings.Contains(recorder.Body.String(), "inbox_messages") || errorObject["code"] != "CONFLICT" {
			t.Fatalf("leak not sanitized or code lost: %s", recorder.Body.String())
		}
	}
}

func mustQuote(text string) string {
	encoded, _ := json.Marshal(text)
	return string(encoded)
}

func TestErrorSanitizerKeepsSafeClientMessagesAndHidesServerDetailsInProduction(t *testing.T) {
	safe := `{"error":{"code":"VALIDATION_FAILED","message":"tenantId cannot be empty"}}`
	recorder := httptest.NewRecorder()
	ErrorSanitizer(true, echoHandler(http.StatusBadRequest, safe)).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/x", nil))
	if !strings.Contains(recorder.Body.String(), "tenantId cannot be empty") {
		t.Fatalf("a safe 4xx message must pass through: %s", recorder.Body.String())
	}

	server := `{"error":{"code":"INTERNAL_SERVER_ERROR","message":"dial tcp 10.0.3.7:5432: connection refused"}}`
	production := httptest.NewRecorder()
	ErrorSanitizer(true, echoHandler(http.StatusInternalServerError, server)).ServeHTTP(production, httptest.NewRequest(http.MethodGet, "/x", nil))
	if strings.Contains(production.Body.String(), "10.0.3.7") {
		t.Fatalf("production 5xx details must be hidden: %s", production.Body.String())
	}
	development := httptest.NewRecorder()
	ErrorSanitizer(false, echoHandler(http.StatusInternalServerError, server)).ServeHTTP(development, httptest.NewRequest(http.MethodGet, "/x", nil))
	if !strings.Contains(development.Body.String(), "10.0.3.7") {
		t.Fatalf("development 5xx details stay visible for debugging: %s", development.Body.String())
	}
}

func TestErrorSanitizerLeavesSuccessResponsesUntouched(t *testing.T) {
	recorder := httptest.NewRecorder()
	ErrorSanitizer(true, echoHandler(http.StatusCreated, `{"success":true,"data":{"note":"violates nothing"}}`)).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/x", nil))
	if recorder.Code != http.StatusCreated || !strings.Contains(recorder.Body.String(), "violates nothing") {
		t.Fatalf("2xx responses must pass through unchanged: %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestReadinessChecksKafkaBrokerReachability(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()

	health := NewHealthCoordinator(nil, address)
	health.SetReady(true)
	up := httptest.NewRecorder()
	health.ReadyHandler().ServeHTTP(up, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if up.Code != http.StatusOK {
		t.Fatalf("a reachable broker must be ready, got %d %s", up.Code, up.Body.String())
	}

	_ = listener.Close()
	down := httptest.NewRecorder()
	health.ReadyHandler().ServeHTTP(down, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if down.Code != http.StatusServiceUnavailable || !strings.Contains(down.Body.String(), "kafka unreachable") {
		t.Fatalf("an unreachable broker must fail readiness with 503, got %d %s", down.Code, down.Body.String())
	}
}

func TestParseBrokersTrimsAndDropsEmptyEntries(t *testing.T) {
	brokers := ParseBrokers(" a:9092, ,b:9092 ,")
	if len(brokers) != 2 || brokers[0] != "a:9092" || brokers[1] != "b:9092" {
		t.Fatalf("unexpected brokers %v", brokers)
	}
	if len(ParseBrokers("")) != 0 {
		t.Fatal("an empty value means no broker check")
	}
}

func TestDrainCancelsStragglingHandlersAfterTheDeadline(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	handlerContext, cancelHandlers := context.WithCancel(context.Background())
	server := &http.Server{
		BaseContext: func(net.Listener) context.Context { return handlerContext },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-r.Context().Done()
			close(cancelled)
		}),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if requestErr == nil {
			_ = response.Body.Close()
		}
	}()
	<-started

	drainContext, cancelDrain := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelDrain()
	if err = drainServer(drainContext, server, cancelHandlers); err == nil {
		t.Fatal("a handler that outlives the drain deadline must make the drain report an error")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the straggling handler must receive a cancellation signal before the hard stop")
	}
}

func TestDrainWaitsForHandlersThatFinishInTime(t *testing.T) {
	finished := make(chan struct{})
	handlerContext, cancelHandlers := context.WithCancel(context.Background())
	server := &http.Server{
		BaseContext: func(net.Listener) context.Context { return handlerContext },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			if r.Context().Err() != nil {
				t.Error("a handler finishing within the deadline must not be cancelled")
			}
			close(finished)
		}),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if requestErr == nil {
			_ = response.Body.Close()
		}
	}()
	time.Sleep(30 * time.Millisecond)
	drainContext, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDrain()
	if err = drainServer(drainContext, server, cancelHandlers); err != nil {
		t.Fatalf("a drain that finishes in time must succeed, got %v", err)
	}
	<-finished
}

func writeJWKS(t *testing.T, key *rsa.PrivateKey) string {
	t.Helper()
	encode := func(value *big.Int) string { return base64.RawURLEncoding.EncodeToString(value.Bytes()) }
	document, _ := json.Marshal(map[string]any{"keys": []map[string]string{
		{"kty": "RSA", "kid": "k1", "use": "sig", "n": encode(key.N), "e": encode(big.NewInt(int64(key.E)))},
	}})
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func signedToken(t *testing.T, key *rsa.PrivateKey, tenant string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": "https://issuer", "aud": "dtx", "sub": "usr_1", "tenant_id": tenant, "exp": time.Now().Add(time.Minute).Unix(),
	})
	token.Header["kid"] = "k1"
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestBuildAuthWrapper_Modes(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.AuthConfig{Mode: "jwt", JWKSFile: writeJWKS(t, key), Issuer: "https://issuer", Audience: "dtx"}
	wrap, err := BuildAuthWrapper(cfg, "production", time.Now)
	if err != nil {
		t.Fatalf("expected wrapper, got %v", err)
	}
	protected := wrap(echoHandler(http.StatusOK, `{"ok":true}`))
	anonymous := httptest.NewRecorder()
	protected.ServeHTTP(anonymous, httptest.NewRequest(http.MethodGet, "/api/v1/sagas", nil))
	if anonymous.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for anonymous call, got %d", anonymous.Code)
	}
	authed := httptest.NewRequest(http.MethodGet, "/api/v1/sagas", nil)
	authed.Header.Set("Authorization", "Bearer "+signedToken(t, key, "tenant_a"))
	allowed := httptest.NewRecorder()
	protected.ServeHTTP(allowed, authed)
	if allowed.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid token, got %d: %s", allowed.Code, allowed.Body.String())
	}
}

func TestBuildAuthWrapper_FailsClosedOnBadConfiguration(t *testing.T) {
	dir := t.TempDir()
	broken := filepath.Join(dir, "broken.json")
	_ = os.WriteFile(broken, []byte(`{"keys":[]}`), 0o600)
	cases := map[string]config.AuthConfig{
		"missing_file":     {Mode: "jwt", JWKSFile: filepath.Join(dir, "absent.json"), Issuer: "i", Audience: "a"},
		"empty_key_set":    {Mode: "jwt", JWKSFile: broken, Issuer: "i", Audience: "a"},
		"missing_audience": {Mode: "jwt", JWKSFile: broken, Issuer: "i"},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildAuthWrapper(cfg, "production", time.Now); err == nil {
				t.Fatal("bad auth configuration must stop the boot")
			}
		})
	}
}

func TestBuildAuthWrapper_DisabledModeIsPassThroughOutsideProduction(t *testing.T) {
	wrap, err := BuildAuthWrapper(config.AuthConfig{}, "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	wrap(echoHandler(http.StatusOK, `{}`)).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/x", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("disabled mode must pass through, got %d", recorder.Code)
	}
}

func TestRun_ServeRefusesSuperuserDatabaseInProduction(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("ENV", "production")
	t.Setenv("AUTH_JWKS_FILE", writeJWKS(t, key))
	t.Setenv("AUTH_ISSUER", "https://issuer")
	t.Setenv("AUTH_AUDIENCE", "dtx")
	t.Setenv("DB_HOST", "db.internal")
	t.Setenv("DB_USER", "postgres")
	t.Setenv("DB_PASSWORD", "secret")
	t.Setenv("DB_RLS_ENFORCED", "true")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
	t.Setenv("KAFKA_SASL_USERNAME", "dtx")
	t.Setenv("KAFKA_SASL_PASSWORD", "secret")
	var output strings.Builder
	log.SetOutput(&output)
	defer log.SetOutput(os.Stderr)
	if code := Run(ServiceSpec{}, []string{"--config", filepath.Join(t.TempDir(), "none.yaml"), "serve"}, nil); code != ExitConfig {
		t.Fatalf("expected ExitConfig, got %d", code)
	}
	if !strings.Contains(output.String(), "restricted login role") {
		t.Fatalf("expected the superuser refusal to be logged, got %q", output.String())
	}
}

func writeTopicsManifest(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "topics.yaml")
	content := "topics:\n  - {name: " + name + ", partitions: 2, replication_factor: 1}\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRun_ProvisionTopicsCommandCreatesTheManifestTopics(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:9092"
	}
	conn, err := net.DialTimeout("tcp", brokers, time.Second)
	if err != nil {
		t.Skipf("kafka is not reachable: %v", err)
	}
	_ = conn.Close()
	topic := "chassis-provision-" + strings.ReplaceAll(time.Now().Format("150405.000000"), ".", "")
	t.Setenv("KAFKA_BROKERS", brokers)
	t.Setenv("KAFKA_TOPICS_MANIFEST", writeTopicsManifest(t, topic))
	args := []string{"--config", filepath.Join(t.TempDir(), "none.yaml"), "provision-topics"}
	if code := Run(ServiceSpec{}, args, nil); code != ExitSuccess {
		t.Fatalf("expected success, got %d", code)
	}
	if code := Run(ServiceSpec{}, args, nil); code != ExitSuccess {
		t.Fatalf("provisioning must be idempotent, got %d", code)
	}
}

func TestRun_ProvisionTopicsFailsWithoutBrokersOrWithAnUnreachableCluster(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	args := []string{"--config", filepath.Join(t.TempDir(), "none.yaml"), "provision-topics"}
	if code := Run(ServiceSpec{}, args, nil); code != ExitDependency {
		t.Fatalf("no brokers must fail with ExitDependency, got %d", code)
	}
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_TOPICS_MANIFEST", writeTopicsManifest(t, "unreachable"))
	if code := Run(ServiceSpec{}, args, nil); code != ExitDependency {
		t.Fatalf("an unreachable cluster must fail with ExitDependency, got %d", code)
	}
}

func TestRun_ServeRefusesProductionWithoutKafkaBrokers(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("ENV", "production")
	t.Setenv("AUTH_JWKS_FILE", writeJWKS(t, key))
	t.Setenv("AUTH_ISSUER", "https://issuer")
	t.Setenv("AUTH_AUDIENCE", "dtx")
	t.Setenv("KAFKA_BROKERS", "")
	var output strings.Builder
	log.SetOutput(&output)
	defer log.SetOutput(os.Stderr)
	if code := Run(ServiceSpec{}, []string{"--config", filepath.Join(t.TempDir(), "none.yaml"), "serve"}, nil); code != ExitConfig {
		t.Fatalf("expected ExitConfig, got %d", code)
	}
	if !strings.Contains(output.String(), "KAFKA_BROKERS is required") {
		t.Fatalf("expected the broker requirement in the log, got %q", output.String())
	}
}

func adminServer(t *testing.T, status int) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/live" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://127.0.0.1:")
}

func TestRun_HealthcheckCommandReportsTheAdminEndpointStatus(t *testing.T) {
	cases := map[string]struct {
		status int
		want   int
	}{"healthy": {http.StatusOK, ExitSuccess}, "no_content": {http.StatusNoContent, ExitSuccess}, "unavailable": {http.StatusServiceUnavailable, ExitRuntime}, "server_error": {http.StatusInternalServerError, ExitRuntime}}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("ADMIN_PORT", adminServer(t, tc.status))
			if code := Run(ServiceSpec{}, []string{"--config", filepath.Join(t.TempDir(), "none.yaml"), "healthcheck"}, nil); code != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, code)
			}
		})
	}
}

func TestRun_HealthcheckFailsWhenNothingListens(t *testing.T) {
	t.Setenv("ADMIN_PORT", "1")
	if code := Run(ServiceSpec{}, []string{"--config", filepath.Join(t.TempDir(), "none.yaml"), "healthcheck"}, nil); code != ExitRuntime {
		t.Fatalf("an unreachable admin endpoint must fail the healthcheck, got %d", code)
	}
}

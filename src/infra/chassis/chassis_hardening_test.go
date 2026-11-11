package chassis

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

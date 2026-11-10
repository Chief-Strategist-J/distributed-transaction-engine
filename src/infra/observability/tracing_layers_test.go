package observability_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	interventionmachines "distributed-transaction-engine/src/features/manual-intervention/domain/machines"
	interventionrules "distributed-transaction-engine/src/features/manual-intervention/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })
	return recorder
}

func attributeOf(span sdktrace.ReadOnlySpan, key string) string {
	for _, candidate := range span.Attributes() {
		if string(candidate.Key) == key {
			return candidate.Value.Emit()
		}
	}
	return ""
}

func findSpan(spans []sdktrace.ReadOnlySpan, namePrefix string) sdktrace.ReadOnlySpan {
	for _, span := range spans {
		if len(span.Name()) >= len(namePrefix) && span.Name()[:len(namePrefix)] == namePrefix {
			return span
		}
	}
	return nil
}

func TestLayerFromPath(t *testing.T) {
	cases := []struct {
		path    string
		feature string
		layer   string
	}{
		{"/x/src/features/saga-orchestration/service/saga.service.go", "saga-orchestration", observability.LayerService},
		{"/x/src/features/manual-intervention/domain/rules/m.rules.go", "manual-intervention", observability.LayerRules},
		{"/x/src/features/manual-intervention/domain/machines/m.machine.go", "manual-intervention", observability.LayerMachine},
		{"/x/src/features/a/domain/workflows/w.go", "a", observability.LayerWorkflow},
		{"/x/src/features/a/infrastructure/ports/p.go", "a", observability.LayerPort},
		{"/x/src/features/a/infrastructure/adapters/a.postgres.go", "a", observability.LayerAdapter},
		{"/x/src/features/a/contracts/a.schema.go", "a", observability.LayerSchema},
		{"/x/src/api/rest/v1/handlers/dead-letter-queue.handler.go", "dead-letter-queue", observability.LayerHandler},
		{"/x/src/api/rest/v1/routers/dead-letter-queue.router.go", "dead-letter-queue", observability.LayerRouter},
		{"/x/src/infra/chassis/runner.go", "", observability.LayerInfra},
		{"/elsewhere/file.go", "", observability.LayerUnknown},
	}
	for _, tc := range cases {
		location := observability.LayerFromPath(tc.path)
		if location.Feature != tc.feature || location.Layer != tc.layer {
			t.Fatalf("%s: got %+v, want feature %q layer %q", tc.path, location, tc.feature, tc.layer)
		}
	}
}

func TestSpanWrapperPositiveAndErrorPaths(t *testing.T) {
	recorder := installRecorder(t)
	tracer := observability.NewTracer("wrapper-test")
	ctx, parent := tracer.StartSpan(context.Background(), "parent")
	_, ok := tracer.StartSpan(ctx, "positive")
	ok.End()
	_, failed := tracer.StartSpan(ctx, "negative")
	observability.RecordError(failed, context.DeadlineExceeded)
	failed.End()
	parent.End()

	spans := recorder.Ended()
	positive, negative := findSpan(spans, "positive"), findSpan(spans, "negative")
	if positive.Status().Code != codes.Ok || attributeOf(positive, observability.AttrOutcome) != observability.OutcomeOK {
		t.Fatalf("positive span must be OK, got %v", positive.Status())
	}
	if negative.Status().Code != codes.Error || attributeOf(negative, observability.AttrOutcome) != observability.OutcomeError || len(negative.Events()) == 0 {
		t.Fatalf("negative span must be ERROR with a recorded exception, got %v", negative.Status())
	}
	if positive.Parent().SpanID() != findSpan(spans, "parent").SpanContext().SpanID() {
		t.Fatal("child span must nest under its parent")
	}
	if attributeOf(positive, observability.AttrLayer) != observability.LayerInfra {
		t.Fatalf("layer must be derived from the caller path, got %q", attributeOf(positive, observability.AttrLayer))
	}
}

func TestTraceHelpersRecordOutcome(t *testing.T) {
	recorder := installRecorder(t)
	tracer := observability.NewTracer("helper-test")
	if err := tracer.Trace(context.Background(), "ok-step", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := observability.TraceValue(context.Background(), tracer, "failing-step", func(context.Context) (int, error) { return 0, context.Canceled })
	if err == nil {
		t.Fatal("expected the error to propagate")
	}
	spans := recorder.Ended()
	if findSpan(spans, "ok-step").Status().Code != codes.Ok || findSpan(spans, "failing-step").Status().Code != codes.Error {
		t.Fatal("helper spans must reflect their outcome")
	}
}

func runThroughMiddleware(handler http.HandlerFunc, method string, path string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/widgets", handler)
	response := httptest.NewRecorder()
	observability.ObservabilityMiddleware("middleware-test", mux).ServeHTTP(response, httptest.NewRequest(method, path, nil))
	return response
}

func TestMiddlewareProducesEndToEndSpansOnSuccess(t *testing.T) {
	recorder := installRecorder(t)
	tracer := observability.NewTracer("feature-handler")
	response := runThroughMiddleware(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.StartLayerSpan(r.Context(), observability.LayerHandler, "widgets.handle")
		defer span.End()
		_, serviceSpan := tracer.StartSpan(ctx, "widgets.service")
		serviceSpan.End()
		w.WriteHeader(http.StatusOK)
	}, http.MethodGet, "/api/v1/widgets")

	spans := recorder.Ended()
	root, router, handler, service := findSpan(spans, "GET /api/v1/widgets"), findSpan(spans, "router "), findSpan(spans, "widgets.handle"), findSpan(spans, "widgets.service")
	if root == nil || router == nil || handler == nil || service == nil {
		t.Fatalf("expected root, router, handler and service spans, got %d spans", len(spans))
	}
	if root.SpanKind() != trace.SpanKindServer || attributeOf(root, "http.route") != "/api/v1/widgets" {
		t.Fatal("root span must be a SERVER span carrying the matched route")
	}
	if router.Parent().SpanID() != root.SpanContext().SpanID() || handler.Parent().SpanID() != router.SpanContext().SpanID() || service.Parent().SpanID() != handler.SpanContext().SpanID() {
		t.Fatal("spans must chain root -> router -> handler -> service")
	}
	for _, span := range []sdktrace.ReadOnlySpan{root, router, handler, service} {
		if span.Status().Code != codes.Ok {
			t.Fatalf("%s must be OK on the success path, got %v", span.Name(), span.Status())
		}
	}
	if response.Header().Get("traceparent") == "" || response.Header().Get("X-Trace-Id") == "" {
		t.Fatal("response must carry trace headers")
	}
}

func TestMiddlewareMarksErrorPathAcrossTransportSpans(t *testing.T) {
	recorder := installRecorder(t)
	tracer := observability.NewTracer("feature-handler")
	response := runThroughMiddleware(func(w http.ResponseWriter, r *http.Request) {
		_, span := tracer.StartLayerSpan(r.Context(), observability.LayerHandler, "widgets.handle")
		defer span.End()
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"success":false,"statusCode":409,"error":{"code":"CONFLICT","message":"widget already exists"}}`))
	}, http.MethodPost, "/api/v1/widgets")

	if response.Code != http.StatusConflict {
		t.Fatalf("unexpected status %d", response.Code)
	}
	spans := recorder.Ended()
	root, router, handler := findSpan(spans, "POST /api/v1/widgets"), findSpan(spans, "router "), findSpan(spans, "widgets.handle")
	for _, span := range []sdktrace.ReadOnlySpan{root, router, handler} {
		if span.Status().Code != codes.Error {
			t.Fatalf("%s must be ERROR on a 409 response, got %v", span.Name(), span.Status())
		}
	}
	if attributeOf(root, "error.code") != "CONFLICT" || attributeOf(root, "error.message") != "widget already exists" || attributeOf(root, "http.status_code") != "409" {
		t.Fatalf("root span must carry the error envelope, got code %q message %q", attributeOf(root, "error.code"), attributeOf(root, "error.message"))
	}
}

func TestMiddlewareRecoversPanicAsErrorTrace(t *testing.T) {
	recorder := installRecorder(t)
	response := runThroughMiddleware(func(http.ResponseWriter, *http.Request) { panic("boom") }, http.MethodGet, "/api/v1/widgets")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("panic must become a 500, got %d", response.Code)
	}
	root := findSpan(recorder.Ended(), "GET /api/v1/widgets")
	if root == nil || root.Status().Code != codes.Error || len(root.Events()) == 0 {
		t.Fatal("root span must record the panic as an exception with ERROR status")
	}
}

func TestMiddlewareUnmatchedRouteIsAnErrorTrace(t *testing.T) {
	recorder := installRecorder(t)
	response := runThroughMiddleware(func(http.ResponseWriter, *http.Request) {}, http.MethodGet, "/api/v1/missing")
	if response.Code != http.StatusNotFound {
		t.Fatalf("expected the mux 404, got %d", response.Code)
	}
	if root := findSpan(recorder.Ended(), "GET unmatched"); root == nil || root.Status().Code != codes.Error {
		t.Fatal("an unmatched route must produce an ERROR root span")
	}
}

func TestTracedPostgresSpans(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not set")
	}
	recorder := installRecorder(t)
	db, err := observability.OpenTracedPostgres(dsn)
	if err != nil || db.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	defer db.Close()
	tracer := observability.NewTracer("sql-test")
	ctx, parent := tracer.StartSpan(context.Background(), "adapter-call", attribute.String("case", "sql"))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pq.StringArray
	if err = tx.QueryRowContext(ctx, "SELECT $1::text[]", pq.Array([]string{"a", "b"})).Scan(&decoded); err != nil || len(decoded) != 2 {
		t.Fatalf("array arguments must pass through the traced driver: %v %v", decoded, err)
	}
	_, failure := tx.ExecContext(ctx, "SELECT * FROM table_that_does_not_exist_tq")
	_ = tx.Rollback()
	parent.End()
	if failure == nil {
		t.Fatal("expected the bad statement to fail")
	}

	spans := recorder.Ended()
	begin, selectSpan, rollback := findSpan(spans, "db BEGIN"), findSpan(spans, "db SELECT"), findSpan(spans, "db ROLLBACK")
	if begin == nil || selectSpan == nil || rollback == nil {
		t.Fatalf("expected BEGIN, SELECT and ROLLBACK spans, got %d spans", len(spans))
	}
	if selectSpan.Parent().SpanID() != findSpan(spans, "adapter-call").SpanContext().SpanID() || selectSpan.SpanKind() != trace.SpanKindClient {
		t.Fatal("SQL spans must be CLIENT spans nested under the calling span")
	}
	if selectSpan.Status().Code != codes.Ok || attributeOf(selectSpan, "db.system") != "postgresql" {
		t.Fatalf("successful statement must be OK, got %v", selectSpan.Status())
	}
	var failedSpan sdktrace.ReadOnlySpan
	for _, span := range spans {
		if attributeOf(span, "db.sql.table") == "table_that_does_not_exist_tq" {
			failedSpan = span
		}
	}
	if failedSpan == nil || failedSpan.Status().Code != codes.Error || attributeOf(failedSpan, "db.error.sqlstate") != "42P01" {
		t.Fatal("failed statement must be an ERROR span carrying the SQLSTATE")
	}
}

func TestRulesAndMachinesJoinTheRequestTrace(t *testing.T) {
	recorder := installRecorder(t)
	tracer := observability.NewTracer("layers-test")
	rulesSet := interventionrules.NewManualInterventionRules()
	machine := interventionmachines.NewManualInterventionMachine()

	if err := rulesSet.ValidateOperatorRole("viewer"); err == nil {
		t.Fatal("expected a rules error")
	}
	if len(recorder.Ended()) != 0 {
		t.Fatal("rules called outside any span must not create orphan spans")
	}

	ctx, service := tracer.StartLayerSpan(context.Background(), observability.LayerService, "service.intervene")
	_ = ctx
	if err := rulesSet.ValidateOperatorRole("admin:saga:intervene"); err != nil {
		t.Fatal(err)
	}
	if err := rulesSet.ValidateOperatorRole("viewer"); err == nil {
		t.Fatal("expected a rules error")
	}
	_ = machine.ValidateActionEligibility("FORCE_RETRY", "STUCK")
	service.End()

	spans := recorder.Ended()
	serviceSpan := findSpan(spans, "service.intervene")
	positive, negative := findSpan(spans, "rules: ManualInterventionRules.ValidateOperatorRole"), (sdktrace.ReadOnlySpan)(nil)
	for _, span := range spans {
		if span.Name() == "rules: ManualInterventionRules.ValidateOperatorRole" && span.Status().Code == codes.Error {
			negative = span
		}
	}
	if positive == nil || negative == nil || findSpan(spans, "machine: ManualInterventionMachine.ValidateActionEligibility") == nil {
		t.Fatalf("expected rules (ok and error) and machine spans, got %d spans", len(spans))
	}
	if positive.Parent().SpanID() != serviceSpan.SpanContext().SpanID() || attributeOf(positive, observability.AttrLayer) != observability.LayerRules {
		t.Fatal("rules spans must be children of the active service span with layer=rules")
	}
}

func TestExpectedLookupMissesAreNotErrorSpansButClientNotFoundsAre(t *testing.T) {
	recorder := installRecorder(t)
	tracer := observability.NewTracer("miss-test")
	ctx, service := tracer.StartLayerSpan(context.Background(), observability.LayerService, "service.get")
	_, adapter := tracer.StartLayerSpan(ctx, observability.LayerAdapter, "adapter.find")
	observability.RecordError(adapter, appErrors.NewNotFoundError("not found"))
	adapter.End()
	_, failing := tracer.StartLayerSpan(ctx, observability.LayerAdapter, "adapter.broken")
	observability.RecordError(failing, appErrors.NewInternalError("database exploded"))
	failing.End()
	observability.RecordError(service, appErrors.NewNotFoundError("saga not found"))
	service.End()

	spans := recorder.Ended()
	miss, broken, served := findSpan(spans, "adapter.find"), findSpan(spans, "adapter.broken"), findSpan(spans, "service.get")
	if miss.Status().Code != codes.Ok || attributeOf(miss, observability.AttrOutcome) != observability.OutcomeNotFound || attributeOf(miss, "error.expected") != "true" {
		t.Fatalf("an adapter lookup miss must be an OK span marked not_found, got %v", miss.Status())
	}
	if len(miss.Events()) == 0 || miss.Events()[0].Name != "lookup.miss" {
		t.Fatal("the miss must still be visible as a span event")
	}
	if broken.Status().Code != codes.Error {
		t.Fatal("a real adapter failure must stay an ERROR span")
	}
	if served.Status().Code != codes.Error {
		t.Fatal("a 404 returned to the caller from the service layer must stay an ERROR span")
	}
}

func TestSanitizeTextRedactsSensitiveValues(t *testing.T) {
	cases := map[string]string{
		"user jane.doe@example.com failed":                           "user [redacted] failed",
		"Key (tenant_id, external_id)=(t1, order-7) already exists":  "Key ([redacted]) already exists",
		"card 4111 1111 1111 1111 declined":                          "card [redacted] declined",
		"password=hunter2 rejected":                                  "password=[redacted] rejected",
		"token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln is invalid": "token [redacted] is invalid",
		"Authorization: Bearer abc.def.ghi":                          "Authorization=[redacted]",
		"order 1234567890123 not found":                              "order [redacted] not found",
		"saga 0b1f6d3e-2c4a-4b8f-9a3e-1d2c3b4a5f60 not found":        "saga 0b1f6d3e-2c4a-4b8f-9a3e-1d2c3b4a5f60 not found",
	}
	for input, want := range cases {
		if got := observability.SanitizeText(input); got != want {
			t.Fatalf("SanitizeText(%q) = %q, want %q", input, got, want)
		}
	}
	if long := observability.SanitizeText(strings.Repeat("x", 1000)); len(long) > 310 {
		t.Fatalf("messages must be truncated, got %d bytes", len(long))
	}
}

func TestSpansNeverCarryRawSensitiveErrorText(t *testing.T) {
	recorder := installRecorder(t)
	tracer := observability.NewTracer("redaction-test")
	_, span := tracer.StartSpan(context.Background(), "leaky")
	observability.RecordError(span, errors.New("lookup failed for jane.doe@example.com password=hunter2"))
	span.End()
	recorded := findSpan(recorder.Ended(), "leaky")
	if strings.Contains(recorded.Status().Description, "jane.doe") || strings.Contains(recorded.Status().Description, "hunter2") {
		t.Fatalf("status description leaked: %q", recorded.Status().Description)
	}
	for _, event := range recorded.Events() {
		for _, attr := range event.Attributes {
			if strings.Contains(attr.Value.Emit(), "jane.doe") || strings.Contains(attr.Value.Emit(), "hunter2") {
				t.Fatalf("event attribute leaked: %s=%s", attr.Key, attr.Value.Emit())
			}
		}
	}
}

func TestTracingSettingsFromEnvironment(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.internal:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer abc, x-team=core")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	t.Setenv("ENV", "production")
	settings := observability.LoadTracingSettings("svc")
	if settings.Endpoint != "collector.internal:4318" || settings.Insecure || settings.Environment != "production" || settings.SamplerArg != 0.25 {
		t.Fatalf("unexpected settings: %+v", settings)
	}
	if settings.Headers["authorization"] != "Bearer abc" || settings.Headers["x-team"] != "core" {
		t.Fatalf("headers not parsed: %v", settings.Headers)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4318")
	if plain := observability.LoadTracingSettings("svc"); plain.Endpoint != "127.0.0.1:4318" || !plain.Insecure {
		t.Fatalf("a bare host:port must be accepted: %+v", plain)
	}
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "7")
	if invalid := observability.LoadTracingSettings("svc"); invalid.SamplerArg != 1.0 {
		t.Fatalf("an out of range ratio must fall back to 1.0, got %v", invalid.SamplerArg)
	}
}

func TestSamplerHonoursRatioAndParent(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0")
	never := observability.LoadTracingSettings("svc").Sampler()
	if never.Description() == "" {
		t.Fatal("sampler must describe itself")
	}
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	if observability.LoadTracingSettings("svc").Sampler().Description() != sdktrace.NeverSample().Description() {
		t.Fatal("always_off must map to the never sampler")
	}
}

func TestUnsampledSpansDoNotLeakIntoTheGoroutineRegistry(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })
	tracer := observability.NewTracer("unsampled")
	for iteration := 0; iteration < 1000; iteration++ {
		_, span := tracer.StartSpan(context.Background(), "dropped")
		span.End()
	}
	if observability.ActiveSpanCount() != 0 {
		t.Fatalf("registry leaked %d entries", observability.ActiveSpanCount())
	}
}

func TestSpansEndedOnAnotherGoroutineAreReleased(t *testing.T) {
	installRecorder(t)
	tracer := observability.NewTracer("cross-goroutine")
	_, span := tracer.StartSpan(context.Background(), "handoff")
	done := make(chan struct{})
	go func() {
		span.End()
		close(done)
	}()
	<-done
	if observability.ActiveSpanCount() != 0 {
		t.Fatalf("a span ended on another goroutine must still be released, %d left", observability.ActiveSpanCount())
	}
}

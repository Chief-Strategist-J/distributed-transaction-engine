package observability

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const maxCapturedErrorBody = 2048

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
	onError    func(statusCode int)
	reported   bool
	traceID    string
	headerSet  bool
	annotated  bool
	onEnvelope func(code string, message string)
}

func (w *responseWriterInterceptor) pinTraceHeader() {
	if !w.headerSet && w.traceID != "" {
		w.headerSet = true
		w.Header().Set("X-Trace-Id", w.traceID)
	}
}

func (w *responseWriterInterceptor) WriteHeader(code int) {
	w.pinTraceHeader()
	w.statusCode = code
	if code >= http.StatusBadRequest && !w.reported && w.onError != nil {
		w.reported = true
		w.onError(code)
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriterInterceptor) Write(payload []byte) (int, error) {
	w.pinTraceHeader()
	if w.statusCode >= http.StatusBadRequest && w.body.Len() < maxCapturedErrorBody {
		remaining := maxCapturedErrorBody - w.body.Len()
		if len(payload) < remaining {
			remaining = len(payload)
		}
		w.body.Write(payload[:remaining])
		w.annotateEnvelope()
	}
	return w.ResponseWriter.Write(payload)
}

func (w *responseWriterInterceptor) annotateEnvelope() {
	if w.annotated || w.onEnvelope == nil {
		return
	}
	if code, message := extractErrorEnvelope(w.body.Bytes()); code != "" {
		w.annotated = true
		w.onEnvelope(code, message)
	}
}

func (w *responseWriterInterceptor) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func extractErrorEnvelope(body []byte) (string, string) {
	var envelope struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return "", ""
	}
	if envelope.Error != nil {
		return envelope.Error.Code, envelope.Error.Message
	}
	return envelope.Code, envelope.Message
}

func resolveRoute(next http.Handler, r *http.Request) string {
	mux, ok := next.(*http.ServeMux)
	if !ok {
		return r.URL.Path
	}
	if _, pattern := mux.Handler(r); pattern != "" {
		return pattern
	}
	return "unmatched"
}

func resolveTenant(r *http.Request) string {
	tenantID := r.Header.Get("X-Tenant-Id")
	if tenantID != "" {
		return tenantID
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for index, part := range parts {
		if part == "tenants" && index+1 < len(parts) {
			return parts[index+1]
		}
	}
	return ""
}

func ObservabilityMiddleware(serviceName string, next http.Handler) http.Handler {
	tracer := NewTracer(serviceName)
	logger := GetLogger()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		tenantID := resolveTenant(r)
		if tenantID != "" {
			ctx = WithTenantID(ctx, tenantID)
		}
		ctx, registry := WithRequestRegistry(ctx)
		route := resolveRoute(next, r)

		ctx, rootSpan := tracer.StartServerSpan(ctx, r.Method+" "+route,
			attribute.String("http.method", r.Method),
			attribute.String("http.target", r.URL.Path),
			attribute.String("http.route", route),
			attribute.String("tenant.id", tenantID),
			attribute.String("http.request_id", r.Header.Get("X-Request-Id")),
			attribute.String("correlation.id", r.Header.Get("X-Correlation-Id")),
		)
		traceID, spanID := ExtractTraceAndSpanIDs(ctx)
		if traceID == "" {
			traceID, spanID = generateRandomHex(16), generateRandomHex(8)
		}
		w.Header().Set("X-Trace-Id", traceID)
		w.Header().Set("X-Span-Id", spanID)
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(w.Header()))

		logger.Info(ctx, "HTTP request started", String("method", r.Method), String("path", r.URL.Path), String("tenant_id", tenantID))

		interceptor := &responseWriterInterceptor{ResponseWriter: w, statusCode: http.StatusOK, traceID: traceID}
		interceptor.onError = func(statusCode int) {
			registry.failOpenTransportSpans(fmt.Sprintf("HTTP %d", statusCode))
		}
		interceptor.onEnvelope = registry.annotateTransportError
		serveTraced(ctx, tracer, next, interceptor, r, route, rootSpan, logger, start, tenantID)
	})
}

func serveTraced(ctx context.Context, tracer *DefaultTracer, next http.Handler, interceptor *responseWriterInterceptor, r *http.Request, route string, rootSpan trace.Span, logger *LokiLogger, start time.Time, tenantID string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			handlePanic(ctx, rootSpan, interceptor, recovered, logger)
		}
		finishRootSpan(ctx, rootSpan, interceptor, r, logger, start, tenantID)
	}()
	routerCtx, routerSpan := tracer.StartLayerSpan(ctx, LayerRouter, "router "+route, attribute.String("http.route", route))
	defer routerSpan.End()
	next.ServeHTTP(interceptor, r.WithContext(routerCtx))
	if interceptor.statusCode >= http.StatusBadRequest {
		RecordError(routerSpan, fmt.Errorf("HTTP %d", interceptor.statusCode))
	}
}

func handlePanic(ctx context.Context, rootSpan trace.Span, interceptor *responseWriterInterceptor, recovered any, logger *LokiLogger) {
	err := fmt.Errorf("panic: %v", recovered)
	rootSpan.RecordError(err, trace.WithStackTrace(true))
	logger.Error(ctx, "HTTP handler panic", String("panic", fmt.Sprint(recovered)))
	rootSpan.SetStatus(codes.Error, err.Error())
	if interceptor.statusCode < http.StatusInternalServerError {
		interceptor.statusCode = http.StatusInternalServerError
		interceptor.Header().Set("Content-Type", "application/json")
		interceptor.ResponseWriter.WriteHeader(http.StatusInternalServerError)
		_, _ = interceptor.ResponseWriter.Write([]byte(`{"success":false,"statusCode":500,"data":null,"error":{"code":"INTERNAL_SERVER_ERROR","message":"internal error","retryable":false}}`))
	}
}

func finishRootSpan(ctx context.Context, rootSpan trace.Span, interceptor *responseWriterInterceptor, r *http.Request, logger *LokiLogger, start time.Time, tenantID string) {
	duration := time.Since(start).Milliseconds()
	rootSpan.SetAttributes(attribute.Int("http.status_code", interceptor.statusCode), attribute.Int64("http.duration_ms", duration))
	fields := []Field{String("method", r.Method), String("path", r.URL.Path), Int("status_code", interceptor.statusCode), Int64("duration_ms", duration), String("tenant_id", tenantID)}
	if interceptor.statusCode >= http.StatusBadRequest {
		logger.Error(ctx, "HTTP request error", fields...)
		code, message := extractErrorEnvelope(interceptor.body.Bytes())
		description := fmt.Sprintf("HTTP %d %s", interceptor.statusCode, code)
		rootSpan.SetAttributes(attribute.String("error.code", code), attribute.String("error.message", message))
		rootSpan.SetStatus(codes.Error, strings.TrimSpace(description))
	} else {
		logger.Info(ctx, "HTTP request completed", fields...)
	}
	rootSpan.End()
}

func generateRandomHex(byteLen int) string {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

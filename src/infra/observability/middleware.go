package observability

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterInterceptor) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func ObservabilityMiddleware(serviceName string, next http.Handler) http.Handler {
	tracer := NewTracer(serviceName)
	logger := GetLogger()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()

		tenantID := r.Header.Get("X-Tenant-Id")
		if tenantID == "" {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			for i, p := range parts {
				if p == "tenants" && i+1 < len(parts) {
					tenantID = parts[i+1]
					break
				}
			}
		}

		if tenantID != "" {
			ctx = WithTenantID(ctx, tenantID)
		}

		spanName := r.Method + " " + r.URL.Path
		attrs := []attribute.KeyValue{
			attribute.String("http.method", r.Method),
			attribute.String("http.target", r.URL.Path),
			attribute.String("tenant.id", tenantID),
		}

		ctx, span := tracer.StartSpan(ctx, spanName, attrs...)
		defer span.End()

		traceID, spanID := ExtractTraceAndSpanIDs(ctx)
		if traceID == "" {
			traceID = generateRandomHex(16)
			spanID = generateRandomHex(8)
		}

		w.Header().Set("X-Trace-Id", traceID)
		w.Header().Set("X-Span-Id", spanID)

		logger.Info(ctx, "HTTP request started",
			String("method", r.Method),
			String("path", r.URL.Path),
			String("tenant_id", tenantID),
		)

		writerInterceptor := &responseWriterInterceptor{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(writerInterceptor, r.WithContext(ctx))

		duration := time.Since(start).Milliseconds()
		span.SetAttributes(attribute.Int("http.status_code", writerInterceptor.statusCode))

		if writerInterceptor.statusCode >= 500 {
			logger.Error(ctx, "HTTP request failed",
				String("method", r.Method),
				String("path", r.URL.Path),
				Int("status_code", writerInterceptor.statusCode),
				Int64("duration_ms", duration),
				String("tenant_id", tenantID),
			)
		} else {
			logger.Info(ctx, "HTTP request completed",
				String("method", r.Method),
				String("path", r.URL.Path),
				Int("status_code", writerInterceptor.statusCode),
				Int64("duration_ms", duration),
				String("tenant_id", tenantID),
			)
		}
	})
}

func generateRandomHex(byteLen int) string {
	b := make([]byte, byteLen)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

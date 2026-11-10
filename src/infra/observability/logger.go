package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type LogLevel string

const (
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
)

type Field struct {
	Key   string
	Value interface{}
}

func String(key string, val string) Field {
	return Field{Key: key, Value: val}
}

func Int(key string, val int) Field {
	return Field{Key: key, Value: val}
}

func Int64(key string, val int64) Field {
	return Field{Key: key, Value: val}
}

func Any(key string, val interface{}) Field {
	return Field{Key: key, Value: val}
}

type LokiLogEntry struct {
	Timestamp  string                 `json:"timestamp"`
	Level      string                 `json:"level"`
	Service    string                 `json:"service"`
	Message    string                 `json:"message"`
	TenantID   string                 `json:"tenant_id,omitempty"`
	TraceID    string                 `json:"trace_id,omitempty"`
	SpanID     string                 `json:"span_id,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

type LokiLogger struct {
	mu           sync.Mutex
	writer       io.Writer
	service      string
	otlpEndpoint string
	httpClient   *http.Client
}

var (
	defaultLogger *LokiLogger
	once          sync.Once
)

func GetLogger() *LokiLogger {
	once.Do(func() {
		otlpEp := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		if otlpEp == "" {
			otlpEp = "127.0.0.1:4318"
		}
		defaultLogger = NewLokiLogger(os.Stdout, "distributed-transaction-engine", otlpEp)
	})
	return defaultLogger
}

func NewLokiLogger(writer io.Writer, service string, otlpEndpoint ...string) *LokiLogger {
	ep := ""
	if len(otlpEndpoint) > 0 {
		ep = otlpEndpoint[0]
	}
	return &LokiLogger{
		writer:       writer,
		service:      service,
		otlpEndpoint: ep,
		httpClient:   &http.Client{Timeout: 2 * time.Second},
	}
}

type tenantContextKey struct{}

func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenantID)
}

func ExtractTenantID(ctx context.Context) string {
	if val, ok := ctx.Value(tenantContextKey{}).(string); ok {
		return val
	}
	return ""
}

func (l *LokiLogger) log(ctx context.Context, level LogLevel, msg string, fields ...Field) {
	switch level {
	case LogLevelWarn:
		NoteLogged("warn", msg)
	case LogLevelError:
		NoteLogged("error", msg)
	}
	traceID, spanID := ExtractTraceAndSpanIDs(ctx)
	tenantID := ExtractTenantID(ctx)

	attrs := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		if f.Key == "tenant_id" && tenantID == "" {
			if strVal, ok := f.Value.(string); ok {
				tenantID = strVal
			}
		}
		attrs[f.Key] = f.Value
	}

	now := time.Now().UTC()
	entry := LokiLogEntry{
		Timestamp:  now.Format(time.RFC3339Nano),
		Level:      string(level),
		Service:    l.service,
		Message:    msg,
		TenantID:   tenantID,
		TraceID:    traceID,
		SpanID:     spanID,
		Attributes: attrs,
	}

	raw, err := json.Marshal(entry)
	if err == nil {
		raw = append(raw, '\n')
		l.mu.Lock()
		_, _ = l.writer.Write(raw)
		l.mu.Unlock()
	}

	go l.forwardToOtel(now, level, msg, traceID, spanID, tenantID, attrs)
}

func (l *LokiLogger) forwardToOtel(now time.Time, level LogLevel, msg, traceID, spanID, tenantID string, attrs map[string]interface{}) {
	if l.otlpEndpoint == "" {
		return
	}

	sevNumber := 9 // INFO
	switch level {
	case LogLevelDebug:
		sevNumber = 5
	case LogLevelWarn:
		sevNumber = 13
	case LogLevelError:
		sevNumber = 17
	}

	timeNs := fmt.Sprintf("%d", now.UnixNano())

	recordAttrs := []map[string]interface{}{
		{"key": "service.name", "value": map[string]interface{}{"stringValue": l.service}},
		{"key": "level", "value": map[string]interface{}{"stringValue": string(level)}},
	}
	if tenantID != "" {
		recordAttrs = append(recordAttrs, map[string]interface{}{
			"key": "tenant_id", "value": map[string]interface{}{"stringValue": tenantID},
		})
	}
	for k, v := range attrs {
		recordAttrs = append(recordAttrs, map[string]interface{}{
			"key": k, "value": map[string]interface{}{"stringValue": fmt.Sprintf("%v", v)},
		})
	}

	otlpPayload := map[string]interface{}{
		"resourceLogs": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]interface{}{"stringValue": l.service}},
					},
				},
				"scopeLogs": []map[string]interface{}{
					{
						"scope": map[string]interface{}{"name": "engine-logger"},
						"logRecords": []map[string]interface{}{
							{
								"timeUnixNano":   timeNs,
								"severityNumber": sevNumber,
								"severityText":   string(level),
								"body":           map[string]interface{}{"stringValue": msg},
								"traceId":        traceID,
								"spanId":         spanID,
								"attributes":     recordAttrs,
							},
						},
					},
				},
			},
		},
	}

	payloadBytes, err := json.Marshal(otlpPayload)
	if err != nil {
		return
	}

	url := fmt.Sprintf("http://%s/v1/logs", l.otlpEndpoint)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(payloadBytes))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := l.httpClient.Do(req)
	if err == nil && resp != nil {
		_ = resp.Body.Close()
	}
}

func (l *LokiLogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, LogLevelInfo, msg, fields...)
}

func (l *LokiLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, LogLevelWarn, msg, fields...)
}

func (l *LokiLogger) Error(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, LogLevelError, msg, fields...)
}

func (l *LokiLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, LogLevelDebug, msg, fields...)
}

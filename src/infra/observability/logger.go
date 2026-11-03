package observability

import (
	"context"
	"encoding/json"
	"io"
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
	mu      sync.Mutex
	writer  io.Writer
	service string
}

var (
	defaultLogger *LokiLogger
	once          sync.Once
)

func GetLogger() *LokiLogger {
	once.Do(func() {
		defaultLogger = NewLokiLogger(os.Stdout, "distributed-transaction-engine")
	})
	return defaultLogger
}

func NewLokiLogger(writer io.Writer, service string) *LokiLogger {
	return &LokiLogger{
		writer:  writer,
		service: service,
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

	entry := LokiLogEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		Level:      string(level),
		Service:    l.service,
		Message:    msg,
		TenantID:   tenantID,
		TraceID:    traceID,
		SpanID:     spanID,
		Attributes: attrs,
	}

	raw, err := json.Marshal(entry)
	if err != nil {
		return
	}
	raw = append(raw, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.writer.Write(raw)
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


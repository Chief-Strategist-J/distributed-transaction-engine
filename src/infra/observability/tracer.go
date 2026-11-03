package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type TracerProvider interface {
	Tracer(name string) trace.Tracer
}

type DefaultTracer struct {
	tracer trace.Tracer
}

func NewTracer(name string) *DefaultTracer {
	return &DefaultTracer{
		tracer: otel.Tracer(name),
	}
}

func (t *DefaultTracer) StartSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
	}
	return t.tracer.Start(ctx, spanName, opts...)
}

func RecordError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func ExtractTraceAndSpanIDs(ctx context.Context) (string, string) {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return "", ""
	}
	return span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String()
}

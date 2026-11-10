package observability

import (
	"context"
	"errors"
	"fmt"

	appErrors "distributed-transaction-engine/src/shared/errors"
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
	return t.startLayerSpan(ctx, spanName, trace.SpanKindInternal, LocateCaller(2), attrs)
}

func (t *DefaultTracer) StartServerSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	location := LocateCaller(2)
	location.Layer = LayerHTTP
	return t.startLayerSpan(ctx, spanName, trace.SpanKindServer, location, attrs)
}

func (t *DefaultTracer) StartLayerSpan(ctx context.Context, layer string, spanName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	location := LocateCaller(2)
	location.Layer = layer
	return t.startLayerSpan(ctx, spanName, trace.SpanKindInternal, location, attrs)
}

func (t *DefaultTracer) startLayerSpan(ctx context.Context, spanName string, kind trace.SpanKind, location CodeLocation, attrs []attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append(append([]attribute.KeyValue(nil), attrs...), location.Attributes()...)
	spanCtx, span := t.tracer.Start(ctx, spanName, trace.WithAttributes(attrs...), trace.WithSpanKind(kind))
	wrapped := newLayerSpan(spanCtx, span, location)
	return context.WithValue(spanCtx, layerSpanKey{}, wrapped), wrapped
}

func (t *DefaultTracer) Trace(ctx context.Context, spanName string, work func(context.Context) error, attrs ...attribute.KeyValue) error {
	spanCtx, span := t.startLayerSpan(ctx, spanName, trace.SpanKindInternal, LocateCaller(2), attrs)
	err := work(spanCtx)
	RecordError(span, err)
	span.End()
	return err
}

func TraceValue[T any](ctx context.Context, tracer *DefaultTracer, spanName string, work func(context.Context) (T, error), attrs ...attribute.KeyValue) (T, error) {
	spanCtx, span := tracer.startLayerSpan(ctx, spanName, trace.SpanKindInternal, LocateCaller(2), attrs)
	value, err := work(spanCtx)
	RecordError(span, err)
	span.End()
	return value, err
}

func RecordError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	if wrapped, ok := span.(*layerSpan); ok && wrapped.recordExpectedMiss(err) {
		return
	}
	safe := newSanitizedError(err)
	span.RecordError(safe)
	span.SetStatus(codes.Error, safe.Error())
	span.SetAttributes(errorAttributes(err)...)
}

func errorAttributes(err error) []attribute.KeyValue {
	attributes := []attribute.KeyValue{attribute.String("error.type", fmt.Sprintf("%T", err))}
	var applicationError *appErrors.ApplicationError
	if errors.As(err, &applicationError) {
		attributes = append(attributes,
			attribute.String("error.code", applicationError.CanonicalCode),
			attribute.Int("error.http_status", applicationError.StatusCode),
			attribute.Bool("error.retryable", applicationError.Retryable),
		)
	}
	return attributes
}

func CurrentSpan(ctx context.Context) trace.Span {
	if wrapped, ok := ctx.Value(layerSpanKey{}).(*layerSpan); ok {
		return wrapped
	}
	return trace.SpanFromContext(ctx)
}

func ExtractTraceAndSpanIDs(ctx context.Context) (string, string) {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return "", ""
	}
	return span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String()
}

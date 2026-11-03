package observability

import (
	"context"
	"log"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

type TracerProvider interface {
	Tracer(name string) trace.Tracer
}

type DefaultTracer struct {
	tracer trace.Tracer
}

func InitTracer(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:4318"
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		log.Printf("Warning: failed to initialize OTLP trace exporter: %v. Running in noop trace mode.\n", err)
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", os.Getenv("ENV")),
		),
	)
	if err != nil {
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
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

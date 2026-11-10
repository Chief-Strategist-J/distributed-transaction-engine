package observability

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

const (
	defaultOTLPEndpoint     = "127.0.0.1:4318"
	productionEnvironment   = "production"
	defaultQueueSize        = 8192
	defaultExportBatchSize  = 512
	defaultBatchTimeout     = 5 * time.Second
	defaultExportTimeout    = 10 * time.Second
	attributeValueLimit     = 512
	attributeCountLimit     = 128
	eventCountLimit         = 64
	linkCountLimit          = 16
	attributesPerEventLimit = 16
)

type TracingSettings struct {
	Disabled    bool
	Endpoint    string
	Insecure    bool
	Headers     map[string]string
	Environment string
	Version     string
	InstanceID  string
	SamplerName string
	SamplerArg  float64
}

func envOrDefault(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func normalizeEndpoint(raw string) (endpoint string, insecure bool) {
	trimmed := strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(trimmed, "https://"):
		return strings.TrimPrefix(trimmed, "https://"), false
	case strings.HasPrefix(trimmed, "http://"):
		return strings.TrimPrefix(trimmed, "http://"), true
	}
	return trimmed, true
}

func parseHeaders(raw string) map[string]string {
	headers := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(pair), "=")
		if found && strings.TrimSpace(key) != "" {
			headers[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	return headers
}

func LoadTracingSettings(serviceName string) TracingSettings {
	environment := envOrDefault("ENV", envOrDefault("ENVIRONMENT", "development"))
	endpoint, insecure := normalizeEndpoint(envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", defaultOTLPEndpoint))
	if value := strings.ToLower(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")); value == "true" || value == "false" {
		insecure = value == "true"
	}
	settings := TracingSettings{
		Disabled:    strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true"),
		Endpoint:    endpoint,
		Insecure:    insecure,
		Headers:     parseHeaders(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")),
		Environment: environment,
		Version:     envOrDefault("SERVICE_VERSION", "dev"),
		InstanceID:  envOrDefault("WORKER_ID", serviceName+"-"+strconv.Itoa(os.Getpid())),
		SamplerName: strings.ToLower(envOrDefault("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")),
		SamplerArg:  1.0,
	}
	if argument, err := strconv.ParseFloat(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), 64); err == nil && argument >= 0 && argument <= 1 {
		settings.SamplerArg = argument
	}
	return settings
}

func (s TracingSettings) Sampler() sdktrace.Sampler {
	switch s.SamplerName {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(s.SamplerArg)
	}
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(s.SamplerArg))
}

func (s TracingSettings) SpanLimits() sdktrace.SpanLimits {
	return sdktrace.SpanLimits{
		AttributeValueLengthLimit:   attributeValueLimit,
		AttributeCountLimit:         attributeCountLimit,
		EventCountLimit:             eventCountLimit,
		LinkCountLimit:              linkCountLimit,
		AttributePerEventCountLimit: attributesPerEventLimit,
		AttributePerLinkCountLimit:  attributesPerEventLimit,
	}
}

func (s TracingSettings) exporterOptions() []otlptracehttp.Option {
	options := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(s.Endpoint),
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		otlptracehttp.WithTimeout(defaultExportTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: true, InitialInterval: time.Second, MaxInterval: 5 * time.Second, MaxElapsedTime: 15 * time.Second}),
	}
	if s.Insecure {
		options = append(options, otlptracehttp.WithInsecure())
	}
	if len(s.Headers) > 0 {
		options = append(options, otlptracehttp.WithHeaders(s.Headers))
	}
	return options
}

func (s TracingSettings) resource(ctx context.Context, serviceName string) *resource.Resource {
	res, err := resource.New(ctx,
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(s.Version),
			semconv.ServiceInstanceIDKey.String(s.InstanceID),
			attribute.String("deployment.environment", s.Environment),
			attribute.String("environment", s.Environment),
		),
	)
	if err != nil {
		return resource.Default()
	}
	return res
}

func InitTracer(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	settings := LoadTracingSettings(serviceName)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if settings.Disabled {
		log.Printf("tracing disabled by OTEL_SDK_DISABLED\n")
		return func(context.Context) error { return nil }, nil
	}
	if err := os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+settings.Endpoint); err != nil {
		return nil, err
	}
	exporter, err := otlptracehttp.New(ctx, settings.exporterOptions()...)
	if err != nil {
		log.Printf("Warning: failed to initialize OTLP trace exporter: %v. Running in noop trace mode.\n", err)
		return func(context.Context) error { return nil }, nil
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxQueueSize(defaultQueueSize),
			sdktrace.WithMaxExportBatchSize(defaultExportBatchSize),
			sdktrace.WithBatchTimeout(defaultBatchTimeout),
			sdktrace.WithExportTimeout(defaultExportTimeout),
		),
		sdktrace.WithResource(settings.resource(ctx, serviceName)),
		sdktrace.WithSampler(settings.Sampler()),
		sdktrace.WithRawSpanLimits(settings.SpanLimits()),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

package observability

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	AttrLayer    = "code.layer"
	AttrFeature  = "code.feature"
	AttrFunction = "code.function"
	AttrOutcome  = "outcome"

	LayerHandler  = "handler"
	LayerRouter   = "router"
	LayerService  = "service"
	LayerRules    = "rules"
	LayerMachine  = "machine"
	LayerWorkflow = "workflow"
	LayerPort     = "port"
	LayerAdapter  = "adapter"
	LayerSchema   = "schema"
	LayerInfra    = "infra"
	LayerHTTP     = "http"
	LayerUnknown  = "unknown"
	OutcomeOK     = "ok"
	OutcomeError  = "error"
)

type CodeLocation struct {
	Feature  string
	Layer    string
	Function string
}

func (l CodeLocation) Attributes() []attribute.KeyValue {
	attributes := []attribute.KeyValue{attribute.String(AttrLayer, l.Layer)}
	if l.Feature != "" {
		attributes = append(attributes, attribute.String(AttrFeature, l.Feature))
	}
	if l.Function != "" {
		attributes = append(attributes, attribute.String(AttrFunction, l.Function))
	}
	return attributes
}

var pathCache sync.Map

func LocateCaller(skip int) CodeLocation {
	programCounter, file, _, ok := runtime.Caller(skip)
	if !ok {
		return CodeLocation{Layer: LayerUnknown}
	}
	location := locatePath(file)
	if function := runtime.FuncForPC(programCounter); function != nil {
		location.Function = shortFunctionName(function.Name())
	}
	return location
}

func locatePath(file string) CodeLocation {
	if cached, found := pathCache.Load(file); found {
		return cached.(CodeLocation)
	}
	location := LayerFromPath(file)
	pathCache.Store(file, location)
	return location
}

func shortFunctionName(full string) string {
	if slash := strings.LastIndex(full, "/"); slash >= 0 {
		full = full[slash+1:]
	}
	if dot := strings.Index(full, "."); dot >= 0 {
		full = full[dot+1:]
	}
	return strings.NewReplacer("(*", "", ")", "").Replace(full)
}

func LayerFromPath(file string) CodeLocation {
	normalized := filepath.ToSlash(file)
	if index := strings.Index(normalized, "/src/features/"); index >= 0 {
		return locateFeaturePath(strings.Split(normalized[index+len("/src/features/"):], "/"))
	}
	for marker, layer := range map[string]string{"/src/api/rest/v1/handlers/": LayerHandler, "/src/api/rest/v1/routers/": LayerRouter} {
		if index := strings.Index(normalized, marker); index >= 0 {
			base := filepath.Base(normalized[index+len(marker):])
			return CodeLocation{Feature: strings.SplitN(base, ".", 2)[0], Layer: layer}
		}
	}
	if strings.Contains(normalized, "/src/infra/") {
		return CodeLocation{Layer: LayerInfra}
	}
	return CodeLocation{Layer: LayerUnknown}
}

func locateFeaturePath(segments []string) CodeLocation {
	if len(segments) < 3 {
		return CodeLocation{Layer: LayerUnknown}
	}
	feature, area, sub := segments[0], segments[1], segments[2]
	layers := map[string]string{"service": LayerService, "contracts": LayerSchema}
	if layer, found := layers[area]; found {
		return CodeLocation{Feature: feature, Layer: layer}
	}
	switch {
	case area == "domain" && sub == "rules":
		return CodeLocation{Feature: feature, Layer: LayerRules}
	case area == "domain" && sub == "machines":
		return CodeLocation{Feature: feature, Layer: LayerMachine}
	case area == "domain" && sub == "workflows":
		return CodeLocation{Feature: feature, Layer: LayerWorkflow}
	case area == "infrastructure" && sub == "ports":
		return CodeLocation{Feature: feature, Layer: LayerPort}
	case area == "infrastructure" && sub == "adapters":
		return CodeLocation{Feature: feature, Layer: LayerAdapter}
	}
	return CodeLocation{Feature: feature, Layer: LayerUnknown}
}

type layerSpan struct {
	trace.Span
	mutex    sync.Mutex
	failed   bool
	location CodeLocation
}

func newLayerSpan(ctx context.Context, span trace.Span, location CodeLocation) *layerSpan {
	wrapped := &layerSpan{Span: span, location: location}
	pushActive(ctx, wrapped)
	if registry := requestRegistryFrom(ctx); registry != nil {
		registry.track(wrapped)
	}
	return wrapped
}

func (s *layerSpan) RecordError(err error, options ...trace.EventOption) {
	if err != nil {
		s.markFailed()
	}
	s.Span.RecordError(err, options...)
}

func (s *layerSpan) SetStatus(code codes.Code, description string) {
	if code == codes.Error {
		s.markFailed()
	}
	s.Span.SetStatus(code, description)
}

func (s *layerSpan) markFailed() {
	s.mutex.Lock()
	s.failed = true
	s.mutex.Unlock()
}

func (s *layerSpan) hasFailed() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.failed
}

func (s *layerSpan) End(options ...trace.SpanEndOption) {
	if s.hasFailed() {
		s.Span.SetAttributes(attribute.String(AttrOutcome, OutcomeError))
	} else {
		s.Span.SetStatus(codes.Ok, "")
		s.Span.SetAttributes(attribute.String(AttrOutcome, OutcomeOK))
	}
	s.Span.End(options...)
	popActive(s)
}

type requestRegistry struct {
	mutex sync.Mutex
	spans []*layerSpan
}

type requestRegistryKey struct{}

func WithRequestRegistry(ctx context.Context) (context.Context, *requestRegistry) {
	registry := &requestRegistry{}
	return context.WithValue(ctx, requestRegistryKey{}, registry), registry
}

func requestRegistryFrom(ctx context.Context) *requestRegistry {
	registry, _ := ctx.Value(requestRegistryKey{}).(*requestRegistry)
	return registry
}

func (r *requestRegistry) track(span *layerSpan) {
	r.mutex.Lock()
	r.spans = append(r.spans, span)
	r.mutex.Unlock()
}

func (r *requestRegistry) annotateTransportError(code string, message string) {
	r.mutex.Lock()
	spans := append([]*layerSpan(nil), r.spans...)
	r.mutex.Unlock()
	for _, span := range spans {
		if span.location.Layer == LayerHandler || span.location.Layer == LayerRouter {
			span.SetAttributes(attribute.String("error.code", code), attribute.String("error.message", message))
		}
	}
}

func (r *requestRegistry) failOpenTransportSpans(description string) {
	r.mutex.Lock()
	spans := append([]*layerSpan(nil), r.spans...)
	r.mutex.Unlock()
	for _, span := range spans {
		if span.location.Layer == LayerHandler || span.location.Layer == LayerRouter || span.location.Layer == LayerHTTP {
			span.SetStatus(codes.Error, description)
		}
	}
}

type layerSpanKey struct{}

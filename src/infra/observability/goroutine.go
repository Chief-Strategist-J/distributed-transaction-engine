package observability

import (
	"context"
	"runtime"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	maxGoroutineStack = 256
	goroutinePrefix   = "goroutine "
)

type activeSpan struct {
	ctx  context.Context
	span *layerSpan
}

var (
	activeMutex sync.Mutex
	activeSpans = make(map[uint64][]activeSpan)
)

func goroutineID() uint64 {
	var buffer [64]byte
	count := runtime.Stack(buffer[:], false)
	var id uint64
	for index := len(goroutinePrefix); index < count && buffer[index] >= '0' && buffer[index] <= '9'; index++ {
		id = id*10 + uint64(buffer[index]-'0')
	}
	return id
}

func pushActive(ctx context.Context, span *layerSpan) {
	id := goroutineID()
	activeMutex.Lock()
	stack := append(activeSpans[id], activeSpan{ctx: ctx, span: span})
	if len(stack) > maxGoroutineStack {
		stack = stack[len(stack)-maxGoroutineStack:]
	}
	activeSpans[id] = stack
	activeMutex.Unlock()
}

func popActive(span *layerSpan) {
	id := goroutineID()
	activeMutex.Lock()
	defer activeMutex.Unlock()
	stack := activeSpans[id]
	for index := len(stack) - 1; index >= 0; index-- {
		if stack[index].span == span {
			stack = append(stack[:index], stack[index+1:]...)
			break
		}
	}
	if len(stack) == 0 {
		delete(activeSpans, id)
		return
	}
	activeSpans[id] = stack
}

func currentContext() context.Context {
	id := goroutineID()
	activeMutex.Lock()
	defer activeMutex.Unlock()
	stack := activeSpans[id]
	if len(stack) == 0 {
		return nil
	}
	return stack[len(stack)-1].ctx
}

func adoptCurrent(ctx context.Context) context.Context {
	current := currentContext()
	if current == nil {
		return ctx
	}
	if trace.SpanContextFromContext(current).TraceID() != trace.SpanContextFromContext(ctx).TraceID() {
		return ctx
	}
	return current
}

func noopEnd() {}

func TraceEnter(errPointer *error) func() {
	parent := currentContext()
	if parent == nil {
		return noopEnd
	}
	location := LocateCaller(2)
	name := location.Layer + ": " + location.Function
	attributes := append(location.Attributes(), attribute.Bool("code.instrumented", true))
	spanCtx, raw := otel.Tracer("distributed-transaction-engine/layers").Start(parent, name, trace.WithAttributes(attributes...))
	span := newLayerSpan(spanCtx, raw, location)
	return func() {
		if errPointer != nil {
			RecordError(span, *errPointer)
		}
		span.End()
	}
}

func currentWrapper() *layerSpan {
	id := goroutineID()
	activeMutex.Lock()
	defer activeMutex.Unlock()
	stack := activeSpans[id]
	if len(stack) == 0 {
		return nil
	}
	return stack[len(stack)-1].span
}

func NoteSwallowed(err error) {
	if err == nil {
		return
	}
	wrapper := currentWrapper()
	if wrapper == nil {
		return
	}
	wrapper.AddEvent("error.swallowed", trace.WithAttributes(append(errorAttributes(err), attribute.String("error.message", err.Error()))...))
	wrapper.SetAttributes(attribute.Bool("error.swallowed", true))
	RecordError(wrapper, err)
}

func NoteLogged(level string, message string) {
	wrapper := currentWrapper()
	if wrapper == nil {
		return
	}
	wrapper.AddEvent("log."+level, trace.WithAttributes(attribute.String("log.message", message)))
	if level == "error" {
		wrapper.SetAttributes(attribute.Bool("error.logged", true))
		wrapper.SetStatus(codes.Error, message)
	}
}

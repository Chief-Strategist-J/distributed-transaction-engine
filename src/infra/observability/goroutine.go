package observability

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

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

const shardCount = 64

type registryShard struct {
	mutex sync.Mutex
	spans map[uint64][]activeSpan
}

var (
	shards      [shardCount]registryShard
	activeCount atomic.Int64
)

func init() {
	for index := range shards {
		shards[index].spans = make(map[uint64][]activeSpan)
	}
}

func shardFor(id uint64) *registryShard {
	return &shards[id%shardCount]
}

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
	if !span.Span.IsRecording() {
		return
	}
	id := goroutineID()
	shard := shardFor(id)
	shard.mutex.Lock()
	stack := append(shard.spans[id], activeSpan{ctx: ctx, span: span})
	if len(stack) > maxGoroutineStack {
		stack = stack[len(stack)-maxGoroutineStack:]
	}
	shard.spans[id] = stack
	shard.mutex.Unlock()
	span.markTracked(id)
	activeCount.Add(1)
}

func popActive(span *layerSpan) {
	id, tracked := span.releaseTracked()
	if !tracked {
		return
	}
	activeCount.Add(-1)
	shard := shardFor(id)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	stack := shard.spans[id]
	for index := len(stack) - 1; index >= 0; index-- {
		if stack[index].span == span {
			stack = append(stack[:index], stack[index+1:]...)
			break
		}
	}
	if len(stack) == 0 {
		delete(shard.spans, id)
		return
	}
	shard.spans[id] = stack
}

func topActive() (activeSpan, bool) {
	if activeCount.Load() <= 0 {
		return activeSpan{}, false
	}
	id := goroutineID()
	shard := shardFor(id)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	stack := shard.spans[id]
	if len(stack) == 0 {
		return activeSpan{}, false
	}
	return stack[len(stack)-1], true
}

func currentContext() context.Context {
	if top, found := topActive(); found {
		return top.ctx
	}
	return nil
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
	if top, found := topActive(); found {
		return top.span
	}
	return nil
}

func NoteSwallowed(err error) {
	if err == nil {
		return
	}
	wrapper := currentWrapper()
	if wrapper == nil {
		return
	}
	wrapper.AddEvent("error.swallowed", trace.WithAttributes(append(errorAttributes(err), attribute.String("error.message", SanitizeText(err.Error())))...))
	wrapper.SetAttributes(attribute.Bool("error.swallowed", true))
	RecordError(wrapper, err)
}

func NoteLogged(level string, message string) {
	wrapper := currentWrapper()
	if wrapper == nil {
		return
	}
	wrapper.AddEvent("log."+level, trace.WithAttributes(attribute.String("log.message", SanitizeText(message))))
	if level == "error" {
		wrapper.SetAttributes(attribute.Bool("error.logged", true))
		wrapper.SetStatus(codes.Error, message)
	}
}

func ActiveSpanCount() int64 {
	return activeCount.Load()
}

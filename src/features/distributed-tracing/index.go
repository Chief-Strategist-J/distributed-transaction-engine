package tracing

import (
	"database/sql"

	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/features/distributed-tracing/infrastructure/adapters"
	"distributed-transaction-engine/src/features/distributed-tracing/infrastructure/ports"
	"distributed-transaction-engine/src/features/distributed-tracing/service"
	"distributed-transaction-engine/src/infra/observability"
)

type DistributedTracingService = service.DistributedTracingService
type ServiceDeps = service.ServiceDeps
type TracingRepositoryPort = ports.TracingRepositoryPort
type StartTraceInput = service.StartTraceInput

type TraceContext = contracts.TraceContext
type SpanEvent = contracts.SpanEvent
type SpanLink = contracts.SpanLink
type RecordSpanCommand = contracts.RecordSpanCommand
type RegisterAsyncLinkCommand = contracts.RegisterAsyncLinkCommand
type ResolveAsyncLinkQuery = contracts.ResolveAsyncLinkQuery
type ActivateDeepSessionCommand = contracts.ActivateDeepSessionCommand
type EvaluateSamplingQuery = contracts.EvaluateSamplingQuery

type SpanRecord = contracts.SpanRecord
type DeepSessionRecord = contracts.DeepSessionRecord
type SamplingRuleRecord = contracts.SamplingRuleRecord
type AsyncLinkRecord = contracts.AsyncLinkRecord

func NewService(deps ServiceDeps) *DistributedTracingService {
	return service.NewDistributedTracingService(deps)
}

func NewMemoryRepository() ports.TracingRepositoryPort {
	return adapters.NewTracingMemoryRepositoryAdapter()
}

func NewPostgresRepository(db *sql.DB) ports.TracingRepositoryPort {
	return adapters.NewTracingPostgresRepositoryAdapter(db)
}

func NewDefaultService(repo ports.TracingRepositoryPort) *DistributedTracingService {
	return service.NewDistributedTracingService(service.ServiceDeps{
		Repo:   repo,
		Tracer: observability.NewTracer("distributed-tracing-service"),
	})
}


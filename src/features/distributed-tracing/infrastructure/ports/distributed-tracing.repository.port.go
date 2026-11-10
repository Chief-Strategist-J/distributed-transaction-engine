package ports

import (
	"context"

	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"github.com/google/uuid"
)

type TracingRepositoryPort interface {
	SaveDeepSession(ctx context.Context, record *contracts.DeepSessionRecord) error
	GetActiveDeepSession(ctx context.Context, tenantID string, targetType contracts.DeepTargetType, targetValue string) (*contracts.DeepSessionRecord, error)
	UpdateDeepSessionStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.DeepSessionStatus) error

	SaveSamplingRule(ctx context.Context, record *contracts.SamplingRuleRecord) error
	GetSamplingRule(ctx context.Context, tenantID string) (*contracts.SamplingRuleRecord, error)

	SaveSpan(ctx context.Context, record *contracts.SpanRecord) error
	GetSpansByTraceID(ctx context.Context, tenantID string, traceID string) ([]contracts.SpanRecord, error)
	CountSpansByTraceID(ctx context.Context, tenantID string, traceID string) (int, error)

	SaveAsyncLink(ctx context.Context, record *contracts.AsyncLinkRecord) error
	GetAsyncLink(ctx context.Context, tenantID string, sagaID uuid.UUID, stepName string, linkType contracts.AsyncLinkType) (*contracts.AsyncLinkRecord, error)
}


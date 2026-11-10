package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"sync"
	"time"
)

type TracingMemoryRepositoryAdapter struct {
	mu            sync.RWMutex
	sessions      []contracts.DeepSessionRecord
	samplingRules []contracts.SamplingRuleRecord
	spans         []contracts.SpanRecord
	links         []contracts.AsyncLinkRecord
}

func NewTracingMemoryRepositoryAdapter() *TracingMemoryRepositoryAdapter {
	return &TracingMemoryRepositoryAdapter{
		sessions:      make([]contracts.DeepSessionRecord, 0),
		samplingRules: make([]contracts.SamplingRuleRecord, 0),
		spans:         make([]contracts.SpanRecord, 0),
		links:         make([]contracts.AsyncLinkRecord, 0),
	}
}

func (a *TracingMemoryRepositoryAdapter) SaveDeepSession(ctx context.Context, record *contracts.DeepSessionRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessions = append(a.sessions, *record)
	return nil
}

func (a *TracingMemoryRepositoryAdapter) GetActiveDeepSession(ctx context.Context, tenantID string, targetType contracts.DeepTargetType, targetValue string) (spanResult0 *contracts.DeepSessionRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	now := time.Now().UTC()
	for i := range a.sessions {
		s := a.sessions[i]
		if s.TenantID == tenantID && s.TargetType == targetType && s.TargetValue == targetValue && s.Status == contracts.DeepSessionActive && s.ExpiresAt.After(now) {
			return &s, nil
		}
	}
	return nil, nil
}

func (a *TracingMemoryRepositoryAdapter) UpdateDeepSessionStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.DeepSessionStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.sessions {
		if a.sessions[i].TenantID == tenantID && a.sessions[i].ID == id {
			a.sessions[i].Status = status
			return nil
		}
	}
	return nil
}

func (a *TracingMemoryRepositoryAdapter) SaveSamplingRule(ctx context.Context, record *contracts.SamplingRuleRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.samplingRules = append(a.samplingRules, *record)
	return nil
}

func (a *TracingMemoryRepositoryAdapter) GetSamplingRule(ctx context.Context, tenantID string) (spanResult0 *contracts.SamplingRuleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := range a.samplingRules {
		if a.samplingRules[i].TenantID == tenantID && a.samplingRules[i].Enabled {
			rec := a.samplingRules[i]
			return &rec, nil
		}
	}
	return nil, nil
}

func (a *TracingMemoryRepositoryAdapter) SaveSpan(ctx context.Context, record *contracts.SpanRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.spans = append(a.spans, *record)
	return nil
}

func (a *TracingMemoryRepositoryAdapter) GetSpansByTraceID(ctx context.Context, tenantID string, traceID string) (spanResult0 []contracts.SpanRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var results []contracts.SpanRecord
	for _, s := range a.spans {
		if s.TenantID == tenantID && s.TraceID == traceID {
			results = append(results, s)
		}
	}
	return results, nil
}

func (a *TracingMemoryRepositoryAdapter) CountSpansByTraceID(ctx context.Context, tenantID string, traceID string) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	count := 0
	for _, s := range a.spans {
		if s.TenantID == tenantID && s.TraceID == traceID {
			count++
		}
	}
	return count, nil
}

func (a *TracingMemoryRepositoryAdapter) SaveAsyncLink(ctx context.Context, record *contracts.AsyncLinkRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.links = append(a.links, *record)
	return nil
}

func (a *TracingMemoryRepositoryAdapter) GetAsyncLink(ctx context.Context, tenantID string, sagaID uuid.UUID, stepName string, linkType contracts.AsyncLinkType) (spanResult0 *contracts.AsyncLinkRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := range a.links {
		l := a.links[i]
		if l.TenantID == tenantID && l.SagaID == sagaID && l.StepName == stepName && l.LinkType == linkType {
			return &l, nil
		}
	}
	return nil, nil
}

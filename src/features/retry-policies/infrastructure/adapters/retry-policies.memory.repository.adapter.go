package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/retry-policies/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"fmt"
	"sync"
	"time"
)

type MemoryRetryRepositoryAdapter struct {
	mu       sync.Mutex
	policies map[string]contracts.RetryPolicyConfig
	attempts map[string]bool
}

func NewMemoryRetryRepositoryAdapter() *MemoryRetryRepositoryAdapter {
	return &MemoryRetryRepositoryAdapter{
		policies: make(map[string]contracts.RetryPolicyConfig),
		attempts: make(map[string]bool),
	}
}

func (a *MemoryRetryRepositoryAdapter) RecordStepAttempt(
	ctx context.Context,
	tenantID, sagaID, stepID string,
	attempt int,
	errClass, errMsg, errFp string,
	retryAfter int,
	delayMillis int64,
	nextAttempt *time.Time,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	k := fmt.Sprintf("%s:%s:%s:%d", tenantID, sagaID, stepID, attempt)
	a.attempts[k] = true
	return nil
}

func (a *MemoryRetryRepositoryAdapter) GetPolicy(
	ctx context.Context,
	tenantID, policyName string,
) (spanResult0 *contracts.RetryPolicyConfig, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	k := fmt.Sprintf("%s:%s", tenantID, policyName)
	p, exists := a.policies[k]
	if !exists {
		return nil, nil
	}
	pCopy := p
	return &pCopy, nil
}

func (a *MemoryRetryRepositoryAdapter) SavePolicy(
	ctx context.Context,
	policy contracts.RetryPolicyConfig,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	k := fmt.Sprintf("%s:%s", policy.TenantID, policy.PolicyName)
	a.policies[k] = policy
	return nil
}

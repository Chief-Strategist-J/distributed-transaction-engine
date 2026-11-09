package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
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
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	k := fmt.Sprintf("%s:%s:%s:%d", tenantID, sagaID, stepID, attempt)
	a.attempts[k] = true
	return nil
}

func (a *MemoryRetryRepositoryAdapter) GetPolicy(
	ctx context.Context,
	tenantID, policyName string,
) (*contracts.RetryPolicyConfig, error) {
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
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	k := fmt.Sprintf("%s:%s", policy.TenantID, policy.PolicyName)
	a.policies[k] = policy
	return nil
}


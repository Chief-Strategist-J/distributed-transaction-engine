package ports

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
)

type RepositoryPort interface {
	RecordStepAttempt(
		ctx context.Context,
		tenantID, sagaID, stepID string,
		attempt int,
		errClass, errMsg, errFp string,
		retryAfter int,
		delayMillis int64,
		nextAttempt *time.Time,
	) error
	GetPolicy(ctx context.Context, tenantID, policyName string) (*contracts.RetryPolicyConfig, error)
	SavePolicy(ctx context.Context, policy contracts.RetryPolicyConfig) error
}


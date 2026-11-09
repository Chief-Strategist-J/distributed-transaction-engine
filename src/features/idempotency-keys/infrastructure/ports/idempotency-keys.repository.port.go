package ports

import (
	"context"

	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
)

type RepositoryPort interface {
	Acquire(ctx context.Context, record contracts.IdempotencyRecord) (*contracts.AcquireResult, error)
	GetByScope(ctx context.Context, tenantID, principalID, operationID, idempotencyKey string) (*contracts.IdempotencyRecord, error)
	CommitOutcome(ctx context.Context, tenantID, id string, state contracts.IdempotencyState, snapshot contracts.ResponseSnapshot, resourceRef string) error
}


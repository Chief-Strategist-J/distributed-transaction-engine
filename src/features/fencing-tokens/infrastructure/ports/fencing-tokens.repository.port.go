package ports

import (
	"context"

	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
)

type RepositoryPort interface {
	AcquireOrTakeover(ctx context.Context, tenantID, resourceID, holderID string, durationMs int) (*contracts.FencingLease, error)
	Renew(ctx context.Context, tenantID, resourceID, holderID string, epoch int64, durationMs int) (*contracts.FencingLease, error)
	Release(ctx context.Context, tenantID, resourceID, holderID string, epoch int64) error
	GetLease(ctx context.Context, tenantID, resourceID string) (*contracts.FencingLease, error)
	ExecuteFencedBarrier(ctx context.Context, tenantID, resourceID string, token int64) (*contracts.FencingDecision, error)
	RecordAudit(ctx context.Context, audit contracts.FencingAudit) error
	GetAudits(ctx context.Context, tenantID, resourceID string, limit int) ([]contracts.FencingAudit, error)
}


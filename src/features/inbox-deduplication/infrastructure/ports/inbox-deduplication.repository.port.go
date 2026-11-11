package ports

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
)

type InboxDeduplicationRepositoryPort interface {
	InsertInboxRecord(ctx context.Context, msg *contracts.InboxMessage) (bool, error)
	GetInboxRecord(ctx context.Context, tenantID string, handlerName string, handlerGeneration int, externalID string) (*contracts.InboxMessage, error)
	MarkQuarantined(ctx context.Context, tenantID string, id string, reason string) error
	PruneApplied(ctx context.Context, olderThan time.Time, limit int) (int, error)
}


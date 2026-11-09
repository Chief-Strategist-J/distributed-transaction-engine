package adapters

import (
	"context"
	"fmt"
	"sync"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type MemoryInboxDeduplicationRepositoryAdapter struct {
	mu       sync.RWMutex
	records  map[string]*contracts.InboxMessage
	byPublic map[string]*contracts.InboxMessage
}

func NewMemoryInboxDeduplicationRepositoryAdapter() *MemoryInboxDeduplicationRepositoryAdapter {
	return &MemoryInboxDeduplicationRepositoryAdapter{
		records:  make(map[string]*contracts.InboxMessage),
		byPublic: make(map[string]*contracts.InboxMessage),
	}
}

func (a *MemoryInboxDeduplicationRepositoryAdapter) InsertInboxRecord(ctx context.Context, msg *contracts.InboxMessage) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%d:%s", msg.TenantID, msg.HandlerName, msg.HandlerGeneration, msg.ExternalID)
	if _, exists := a.records[key]; exists {
		return false, nil
	}

	copied := *msg
	a.records[key] = &copied
	a.byPublic[fmt.Sprintf("%s:%s", msg.TenantID, msg.PublicID)] = &copied
	return true, nil
}

func (a *MemoryInboxDeduplicationRepositoryAdapter) GetInboxRecord(
	ctx context.Context,
	tenantID string,
	handlerName string,
	handlerGeneration int,
	externalID string,
) (*contracts.InboxMessage, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%d:%s", tenantID, handlerName, handlerGeneration, externalID)
	msg, exists := a.records[key]
	if !exists {
		return nil, errors.NewNotFoundError("inbox message not found")
	}

	copied := *msg
	return &copied, nil
}

func (a *MemoryInboxDeduplicationRepositoryAdapter) MarkQuarantined(ctx context.Context, tenantID string, id string, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, msg := range a.records {
		if msg.TenantID == tenantID && msg.ID == id {
			msg.Status = contracts.StatusQuarantined
			return nil
		}
	}
	return errors.NewNotFoundError("inbox record to quarantine not found")
}


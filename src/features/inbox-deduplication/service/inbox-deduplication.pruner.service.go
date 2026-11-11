package service

import (
	"context"
	"time"

	"distributed-transaction-engine/src/infra/observability"
)

const maxBatchesPerTick = 200

type InboxPruner struct {
	service   *InboxDeduplicationService
	retention time.Duration
	interval  time.Duration
	batchSize int
}

func NewInboxPruner(service *InboxDeduplicationService, retention time.Duration, interval time.Duration, batchSize int) *InboxPruner {
	return &InboxPruner{service: service, retention: retention, interval: interval, batchSize: batchSize}
}

func (p *InboxPruner) Run(ctx context.Context) {
	defer observability.TraceEnter(nil)()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		p.pruneOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *InboxPruner) pruneOnce(ctx context.Context) {
	deleted, err := p.service.PruneExpired(ctx, p.retention, p.batchSize, maxBatchesPerTick)
	if err != nil {
		observability.GetLogger().Error(ctx, "inbox prune failed", observability.String("error", err.Error()))
		return
	}
	if deleted > 0 {
		observability.GetLogger().Info(ctx, "inbox records pruned", observability.Int("deleted", deleted))
	}
}

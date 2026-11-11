package service

import (
	"context"
	"time"

	"distributed-transaction-engine/src/infra/observability"
)

const maxRelayRoundsPerTick = 20

type OutboxRelay struct {
	service       *OutboxService
	workerID      string
	batchLimit    int
	leaseDuration time.Duration
	interval      time.Duration
}

func NewOutboxRelay(service *OutboxService, workerID string, batchLimit int, leaseDuration time.Duration, interval time.Duration) *OutboxRelay {
	return &OutboxRelay{service: service, workerID: workerID, batchLimit: batchLimit, leaseDuration: leaseDuration, interval: interval}
}

func (r *OutboxRelay) Run(ctx context.Context) {
	defer observability.TraceEnter(nil)()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *OutboxRelay) drain(ctx context.Context) {
	for round := 0; round < maxRelayRoundsPerTick && ctx.Err() == nil; round++ {
		published, err := r.service.RelayAllPending(ctx, r.workerID, r.batchLimit, r.leaseDuration)
		if err != nil {
			observability.GetLogger().Error(ctx, "outbox relay round failed", observability.String("error", err.Error()))
			return
		}
		if published == 0 {
			return
		}
	}
}

package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"sync"
	"time"
)

type MemoryOutboxRepositoryAdapter struct {
	mu     sync.Mutex
	events map[string]*contracts.OutboxEventInstance
}

func NewMemoryOutboxRepositoryAdapter() *MemoryOutboxRepositoryAdapter {
	return &MemoryOutboxRepositoryAdapter{
		events: make(map[string]*contracts.OutboxEventInstance),
	}
}

func (a *MemoryOutboxRepositoryAdapter) InsertOutboxEventTx(
	_ context.Context,
	_ *sql.Tx,
	event *contracts.OutboxEventInstance,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, existing := range a.events {
		if existing.TenantID == event.TenantID &&
			existing.AggregateID == event.AggregateID &&
			existing.AggregateSequence == event.AggregateSequence {
			return errors.NewConflictError("duplicate aggregate sequence")
		}
	}

	cpy := *event
	cpy.Status = contracts.OutboxStatusPending
	cpy.CreatedAt = time.Now().UTC()
	cpy.UpdatedAt = cpy.CreatedAt
	a.events[event.ID] = &cpy
	return nil
}

func (a *MemoryOutboxRepositoryAdapter) ClaimPendingEvents(
	_ context.Context,
	input *contracts.ClaimOutboxEventsInput,
) (spanResult0 []contracts.OutboxEventInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now().UTC()
	leaseExpiry := now.Add(input.LeaseDuration)

	var claimed []contracts.OutboxEventInstance
	for _, e := range a.events {
		if e.TenantID == input.TenantID && e.DestinationTopic == input.Topic && e.Status == contracts.OutboxStatusPending {
			e.Status = contracts.OutboxStatusInFlight
			e.LeaseOwner = input.WorkerID
			e.LeaseExpiresAt = &leaseExpiry
			e.AttemptCount++
			e.UpdatedAt = now
			claimed = append(claimed, *e)
			if len(claimed) >= input.BatchLimit {
				break
			}
		}
	}
	return claimed, nil
}

func (a *MemoryOutboxRepositoryAdapter) ConfirmPublished(
	_ context.Context,
	input *contracts.ConfirmOutboxPublishInput,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	e, ok := a.events[input.EventID]
	if !ok || e.TenantID != input.TenantID || e.Status != contracts.OutboxStatusInFlight || e.LeaseOwner != input.WorkerID {
		return errors.NewConflictError("stale lease or event not in-flight")
	}

	now := time.Now().UTC()
	e.Status = contracts.OutboxStatusPublished
	e.PublishedAt = &now
	e.UpdatedAt = now
	return nil
}

func (a *MemoryOutboxRepositoryAdapter) ReleaseInFlightToPending(
	_ context.Context,
	tenantID string,
	eventID string,
	reason string,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	e, ok := a.events[eventID]
	if !ok || e.TenantID != tenantID || e.Status != contracts.OutboxStatusInFlight {
		return errors.NewConflictError("event not found or not in flight")
	}

	e.Status = contracts.OutboxStatusPending
	e.LeaseOwner = ""
	e.LeaseExpiresAt = nil
	e.LastError = reason
	e.UpdatedAt = time.Now().UTC()
	return nil
}

func (a *MemoryOutboxRepositoryAdapter) ParkEvent(
	_ context.Context,
	input *contracts.ParkOutboxEventInput,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	e, ok := a.events[input.EventID]
	if !ok || e.TenantID != input.TenantID || e.Status != contracts.OutboxStatusInFlight || e.LeaseOwner != input.WorkerID {
		return errors.NewConflictError("event not found or not owned in-flight")
	}

	e.Status = contracts.OutboxStatusParked
	e.LeaseOwner = ""
	e.LeaseExpiresAt = nil
	e.LastError = input.Reason
	e.UpdatedAt = time.Now().UTC()
	return nil
}

func (a *MemoryOutboxRepositoryAdapter) GetEvent(
	_ context.Context,
	tenantID string,
	eventID string,
) (spanResult0 *contracts.OutboxEventInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	e, ok := a.events[eventID]
	if !ok || e.TenantID != tenantID {
		return nil, errors.NewNotFoundError("outbox event not found")
	}
	cpy := *e
	return &cpy, nil
}

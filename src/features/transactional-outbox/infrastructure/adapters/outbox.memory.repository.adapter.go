package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	rules "distributed-transaction-engine/src/features/transactional-outbox/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"sort"
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
	if keyErr := contracts.ValidatePartitionKey(event.SagaID, event.PartitionKey); keyErr != nil {
		return keyErr
	}
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
	candidates := a.relayCandidates(input.TenantID, input.Topic, now)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].AggregateSequence < candidates[j].AggregateSequence })

	var claimed []contracts.OutboxEventInstance
	for _, e := range candidates {
		if len(claimed) >= input.BatchLimit {
			break
		}
		e.Status = contracts.OutboxStatusInFlight
		e.LeaseOwner = input.WorkerID
		e.LeaseExpiresAt = &leaseExpiry
		e.AttemptCount++
		e.UpdatedAt = now
		claimed = append(claimed, *e)
	}
	return claimed, nil
}

func (a *MemoryOutboxRepositoryAdapter) relayCandidates(tenantID string, topic string, now time.Time) []*contracts.OutboxEventInstance {
	lowest := map[string]*contracts.OutboxEventInstance{}
	for _, e := range a.events {
		if e.TenantID != tenantID || e.DestinationTopic != topic || e.Status == contracts.OutboxStatusPublished {
			continue
		}
		if current, seen := lowest[e.AggregateID]; !seen || e.AggregateSequence < current.AggregateSequence {
			lowest[e.AggregateID] = e
		}
	}
	candidates := make([]*contracts.OutboxEventInstance, 0, len(lowest))
	for _, e := range lowest {
		if relayEligible(e, now) {
			candidates = append(candidates, e)
		}
	}
	return candidates
}

func relayEligible(e *contracts.OutboxEventInstance, now time.Time) bool {
	switch e.Status {
	case contracts.OutboxStatusPending:
		return e.NextAttemptAt == nil || !e.NextAttemptAt.After(now)
	case contracts.OutboxStatusInFlight:
		return e.LeaseExpiresAt != nil && e.LeaseExpiresAt.Before(now)
	default:
		return false
	}
}

func (a *MemoryOutboxRepositoryAdapter) ListRelayTargets(
	_ context.Context,
	limit int,
) (spanResult0 []contracts.OutboxRelayTarget, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now().UTC()
	seen := map[contracts.OutboxRelayTarget]bool{}
	targets := make([]contracts.OutboxRelayTarget, 0)
	for _, e := range a.events {
		target := contracts.OutboxRelayTarget{TenantID: e.TenantID, Topic: e.DestinationTopic}
		if relayEligible(e, now) && !seen[target] && len(targets) < limit {
			seen[target] = true
			targets = append(targets, target)
		}
	}
	return targets, nil
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

	retryAt := time.Now().UTC().Add(rules.RelayBackoff(e.AttemptCount))
	e.Status = contracts.OutboxStatusPending
	e.LeaseOwner = ""
	e.LeaseExpiresAt = nil
	e.NextAttemptAt = &retryAt
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

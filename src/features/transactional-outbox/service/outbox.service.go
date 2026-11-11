package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	rules "distributed-transaction-engine/src/features/transactional-outbox/domain/rules"
	ports "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

const relayTargetLimit = 100

type OutboxServiceDeps struct {
	Repo      ports.OutboxRepositoryPort
	Publisher ports.KafkaPublisherPort
	Machine   *machines.OutboxStateMachine
	Tracer    *observability.DefaultTracer
}

type OutboxService struct {
	repo      ports.OutboxRepositoryPort
	publisher ports.KafkaPublisherPort
	machine   *machines.OutboxStateMachine
	tracer    *observability.DefaultTracer
}

func NewOutboxService(deps OutboxServiceDeps) *OutboxService {
	return &OutboxService{
		repo:      deps.Repo,
		publisher: deps.Publisher,
		machine:   deps.Machine,
		tracer:    deps.Tracer,
	}
}

func (s *OutboxService) RelayPendingEvents(
	ctx context.Context,
	tenantID string,
	topic string,
	workerID string,
	batchLimit int,
	leaseDuration time.Duration,
) (spanResult0 int, spanErr error) {
	_, span := s.tracer.StartSpan(ctx, "outbox.relay_pending")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	input := &contracts.ClaimOutboxEventsInput{
		TenantID:      tenantID,
		Topic:         topic,
		WorkerID:      workerID,
		LeaseDuration: leaseDuration,
		BatchLimit:    batchLimit,
	}

	events, err := s.repo.ClaimPendingEvents(ctx, input)
	if err != nil {
		return 0, err
	}

	publishable, requests := s.prepareBatch(ctx, tenantID, workerID, events)
	if len(requests) == 0 {
		return 0, nil
	}
	outcomes := s.publisher.PublishBatch(ctx, requests)

	publishedCount := 0
	for index, event := range publishable {
		if s.settleEvent(ctx, tenantID, workerID, event, outcomes[index]) {
			publishedCount++
		}
	}
	return publishedCount, nil
}

func (s *OutboxService) prepareBatch(ctx context.Context, tenantID string, workerID string, events []contracts.OutboxEventInstance) ([]contracts.OutboxEventInstance, []contracts.PublishRequest) {
	publishable := make([]contracts.OutboxEventInstance, 0, len(events))
	requests := make([]contracts.PublishRequest, 0, len(events))
	for _, event := range events {
		payloadBytes, err := json.Marshal(event.Payload)
		if err != nil {
			observability.NoteSwallowed(s.repo.ParkEvent(ctx, &contracts.ParkOutboxEventInput{
				TenantID: tenantID,
				EventID:  event.ID,
				WorkerID: workerID,
				Reason:   "failed to marshal payload JSON: " + err.Error(),
			}))
			continue
		}
		publishable = append(publishable, event)
		requests = append(requests, contracts.PublishRequest{
			Topic: event.DestinationTopic, PartitionKey: event.PartitionKey, Headers: event.Headers, Payload: payloadBytes,
		})
	}
	return publishable, requests
}

func (s *OutboxService) settleEvent(ctx context.Context, tenantID string, workerID string, event contracts.OutboxEventInstance, pubErr error) bool {
	nextStatus, _ := s.machine.EvaluatePublishOutcome(event.Status, event.AttemptCount, pubErr)
	switch nextStatus {
	case contracts.OutboxStatusPublished:
		confirmInput := &contracts.ConfirmOutboxPublishInput{TenantID: tenantID, EventID: event.ID, WorkerID: workerID}
		return s.repo.ConfirmPublished(ctx, confirmInput) == nil
	case contracts.OutboxStatusParked:
		observability.NoteSwallowed(s.repo.ParkEvent(ctx, &contracts.ParkOutboxEventInput{
			TenantID: tenantID, EventID: event.ID, WorkerID: workerID, Reason: parkReason(event.AttemptCount, pubErr),
		}))
	case contracts.OutboxStatusPending:
		observability.NoteSwallowed(s.repo.ReleaseInFlightToPending(ctx, tenantID, event.ID, pubErr.Error()))
	}
	return false
}

func parkReason(attemptCount int, pubErr error) string {
	if rules.ExceededPublishAttempts(attemptCount) && rules.ClassifyPublishError(pubErr) == rules.ErrorRetriable {
		return fmt.Sprintf("exceeded %d publish attempts: %v", rules.MaxPublishAttempts, pubErr)
	}
	return pubErr.Error()
}

func (s *OutboxService) RelayAllPending(ctx context.Context, workerID string, batchLimit int, leaseDuration time.Duration) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	targets, err := s.repo.ListRelayTargets(ctx, relayTargetLimit)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, target := range targets {
		count, relayErr := s.RelayPendingEvents(ctx, target.TenantID, target.Topic, workerID, batchLimit, leaseDuration)
		if relayErr != nil {
			return total, relayErr
		}
		total += count
	}
	return total, nil
}

func (s *OutboxService) GetOutboxEvent(ctx context.Context, tenantID string, eventID string) (spanResult0 *contracts.OutboxEventInstance, spanErr error) {
	_, span := s.tracer.StartSpan(ctx, "outbox.get_event")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if tenantID == "" || eventID == "" {
		return nil, errors.NewValidationError("tenantId and eventId cannot be empty")
	}
	return s.repo.GetEvent(ctx, tenantID, eventID)
}

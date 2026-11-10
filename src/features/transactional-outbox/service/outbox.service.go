package service

import (
	"context"
	"encoding/json"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	ports "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

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

	publishedCount := 0
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

		pubErr := s.publisher.PublishMessage(ctx, event.DestinationTopic, event.PartitionKey, event.Headers, payloadBytes)
		nextStatus, _ := s.machine.EvaluatePublishOutcome(event.Status, pubErr)

		switch nextStatus {
		case contracts.OutboxStatusPublished:
			confirmInput := &contracts.ConfirmOutboxPublishInput{
				TenantID: tenantID,
				EventID:  event.ID,
				WorkerID: workerID,
			}
			if err := s.repo.ConfirmPublished(ctx, confirmInput); err == nil {
				publishedCount++
			}
		case contracts.OutboxStatusParked:
			observability.NoteSwallowed(s.repo.ParkEvent(ctx, &contracts.ParkOutboxEventInput{
				TenantID: tenantID,
				EventID:  event.ID,
				WorkerID: workerID,
				Reason:   pubErr.Error(),
			}))
		case contracts.OutboxStatusPending:
			observability.NoteSwallowed(s.repo.ReleaseInFlightToPending(ctx, tenantID, event.ID, pubErr.Error()))
		}
	}

	return publishedCount, nil
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

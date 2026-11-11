package service

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	rules "distributed-transaction-engine/src/features/inbox-deduplication/domain/rules"
	ports "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type InboxDeduplicationServiceDeps struct {
	Repo   ports.InboxDeduplicationRepositoryPort
	Tracer *observability.DefaultTracer
}

type InboxDeduplicationService struct {
	repo   ports.InboxDeduplicationRepositoryPort
	tracer *observability.DefaultTracer
}

func NewInboxDeduplicationService(deps InboxDeduplicationServiceDeps) *InboxDeduplicationService {
	return &InboxDeduplicationService{
		repo:   deps.Repo,
		tracer: deps.Tracer,
	}
}

func (s *InboxDeduplicationService) ProcessIncomingMessage(
	ctx context.Context,
	input *contracts.IngestMessageInput,
) (spanResult0 *contracts.IngestDecision, spanErr error) {
	_, span := s.tracer.StartSpan(ctx, "ProcessIncomingMessage")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateIngestMessageInput(input); err != nil {
		return nil, err
	}

	incomingHash, err := contracts.ComputeCanonicalPayloadHash(input.Payload)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	msg := &contracts.InboxMessage{
		ID:                input.ID,
		PublicID:          input.PublicID,
		TenantID:          input.TenantID,
		Source:            input.Source,
		ExternalID:        input.ExternalID,
		CorrelationID:     input.CorrelationID,
		HandlerName:       input.HandlerName,
		HandlerGeneration: input.HandlerGeneration,
		ContentHash:       incomingHash,
		Status:            contracts.StatusApplied,
		AttemptCount:      1,
		ProducerEventTime: input.ProducerEventTime,
		Payload:           input.Payload,
		Version:           1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	inserted, err := s.repo.InsertInboxRecord(ctx, msg)
	if err != nil {
		return nil, err
	}

	if inserted {
		return &contracts.IngestDecision{
			Outcome:      contracts.OutcomeApplied,
			StoredHash:   incomingHash,
			IncomingHash: incomingHash,
		}, nil
	}

	existing, err := s.repo.GetInboxRecord(ctx, input.TenantID, input.HandlerName, input.HandlerGeneration, input.ExternalID)
	if err != nil {
		return nil, err
	}

	outcome := rules.EvaluateDuplicateContent(existing.ContentHash, incomingHash)
	if outcome == contracts.OutcomeIntegrityMismatch {
		observability.NoteSwallowed(s.repo.MarkQuarantined(ctx, input.TenantID, existing.ID, "content hash mismatch detected"))
		return nil, errors.NewConflictError("inbox message integrity failure: event id reused with mismatched payload")
	}

	return &contracts.IngestDecision{
		Outcome:      outcome,
		StoredHash:   existing.ContentHash,
		IncomingHash: incomingHash,
	}, nil
}

func (s *InboxDeduplicationService) GetRecord(
	ctx context.Context,
	tenantID string,
	handlerName string,
	handlerGen int,
	externalID string,
) (spanResult0 *contracts.InboxMessage, spanErr error) {
	_, span := s.tracer.StartSpan(ctx, "GetRecord")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.GetInboxRecord(ctx, tenantID, handlerName, handlerGen, externalID)
}

const defaultPruneBatchSize = 5000

func (s *InboxDeduplicationService) PruneExpired(
	ctx context.Context,
	retention time.Duration,
	batchSize int,
	maxBatches int,
) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if retention <= 0 {
		return 0, errors.NewValidationError("retention must be positive")
	}
	if batchSize <= 0 {
		batchSize = defaultPruneBatchSize
	}
	cutoff := time.Now().UTC().Add(-retention)
	total := 0
	for batch := 0; batch < maxBatches && ctx.Err() == nil; batch++ {
		deleted, err := s.repo.PruneApplied(ctx, cutoff, batchSize)
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted < batchSize {
			break
		}
	}
	return total, nil
}

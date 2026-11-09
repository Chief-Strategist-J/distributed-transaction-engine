package service

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/features/idempotency-keys/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type ServiceDeps struct {
	Repo   ports.RepositoryPort
	Tracer *observability.DefaultTracer
}

type IdempotencyService struct {
	repo   ports.RepositoryPort
	tracer *observability.DefaultTracer
	logger *observability.LokiLogger
}

func NewService(deps ServiceDeps) *IdempotencyService {
	tracer := deps.Tracer
	if tracer == nil {
		tracer = observability.NewTracer("idempotency-keys-service")
	}
	return &IdempotencyService{
		repo:   deps.Repo,
		tracer: tracer,
		logger: observability.GetLogger(),
	}
}

func (s *IdempotencyService) Ingest(
	ctx context.Context,
	input contracts.IngestIdempotencyInput,
) (*contracts.AcquireResult, error) {
	ctx, span := s.tracer.StartSpan(ctx, "idempotency_keys.ingest")
	defer span.End()

	parsedID, parsedCorrID, err := contracts.ValidateIngestInput(input)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	fingerprint, err := contracts.ComputeCanonicalFingerprint(input.OperationID, input.Payload)
	if err != nil {
		observability.RecordError(span, err)
		return nil, appErrors.NewInternalError(err.Error())
	}

	now := time.Now().UTC()
	expiresAt := now.Add(30 * 24 * time.Hour)
	leaseUntil := now.Add(30 * time.Second)

	record := contracts.IdempotencyRecord{
		ID:                 *parsedID,
		PublicID:           input.PublicID,
		TenantID:           input.TenantID,
		Source:             input.Source,
		ExternalID:         input.ExternalID,
		CorrelationID:      *parsedCorrID,
		PrincipalID:        input.PrincipalID,
		OperationID:        input.OperationID,
		IdempotencyKey:     input.IdempotencyKey,
		RequestFingerprint: fingerprint,
		CurrentState:       contracts.StateInProgress,
		LeaseOwner:         "gateway_worker",
		LeaseUntil:         &leaseUntil,
		ExpiresAt:          expiresAt,
	}

	res, err := s.repo.Acquire(ctx, record)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	switch res.Outcome {
	case contracts.OutcomeFingerprintMismatch:
		err := appErrors.NewConflictError("idempotency key reused with different request payload")
		observability.RecordError(span, err)
		return nil, err

	case contracts.OutcomeConflictInProgress:
		err := appErrors.NewConflictError("operation currently in progress with this idempotency key")
		observability.RecordError(span, err)
		return nil, err

	default:
		return res, nil
	}
}

func (s *IdempotencyService) CommitSuccess(
	ctx context.Context,
	tenantID, id string,
	snapshot contracts.ResponseSnapshot,
	resourceRef string,
) error {
	ctx, span := s.tracer.StartSpan(ctx, "idempotency_keys.commit_success")
	defer span.End()

	err := s.repo.CommitOutcome(ctx, tenantID, id, contracts.StateCompleted, snapshot, resourceRef)
	if err != nil {
		observability.RecordError(span, err)
		return err
	}
	return nil
}

func (s *IdempotencyService) CommitRejection(
	ctx context.Context,
	tenantID, id string,
	snapshot contracts.ResponseSnapshot,
) error {
	ctx, span := s.tracer.StartSpan(ctx, "idempotency_keys.commit_rejection")
	defer span.End()

	err := s.repo.CommitOutcome(ctx, tenantID, id, contracts.StateRejectedFinal, snapshot, "")
	if err != nil {
		observability.RecordError(span, err)
		return err
	}
	return nil
}

func (s *IdempotencyService) GetRecord(
	ctx context.Context,
	tenantID, principalID, operationID, idempotencyKey string,
) (*contracts.IdempotencyRecord, error) {
	ctx, span := s.tracer.StartSpan(ctx, "idempotency_keys.get_record")
	defer span.End()

	rec, err := s.repo.GetByScope(ctx, tenantID, principalID, operationID, idempotencyKey)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	return rec, nil
}


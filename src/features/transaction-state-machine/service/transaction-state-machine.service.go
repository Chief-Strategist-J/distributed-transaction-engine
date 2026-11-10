package service

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	machines "distributed-transaction-engine/src/features/transaction-state-machine/domain/machines"
	ports "distributed-transaction-engine/src/features/transaction-state-machine/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type TransactionStateMachineService struct {
	repo         ports.TransactionStateMachineRepositoryPort
	stateMachine *machines.TransactionStateMachine
	logger       *observability.LokiLogger
	tracer       *observability.DefaultTracer
}

func NewTransactionStateMachineService(
	repo ports.TransactionStateMachineRepositoryPort,
	stateMachine *machines.TransactionStateMachine,
) *TransactionStateMachineService {
	return &TransactionStateMachineService{
		repo:         repo,
		stateMachine: stateMachine,
		logger:       observability.GetLogger(),
		tracer:       observability.NewTracer("transaction-state-machine-service"),
	}
}

func (s *TransactionStateMachineService) InitializeState(ctx context.Context, input contracts.InitializeStateInput) (*contracts.SagaStateRecord, error) {
	ctx, span := s.tracer.StartSpan(ctx, "state_machine_service.initialize")
	defer span.End()

	if err := contracts.ValidateInitializeStateInput(input); err != nil {
		observability.RecordError(span, err)
		s.logger.Warn(ctx, "state machine initialize input validation failed", observability.String("error", err.Error()))
		return nil, err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	record := &contracts.SagaStateRecord{
		ID:            input.ID,
		PublicID:      input.PublicID,
		TenantID:      input.TenantID,
		Source:        input.Source,
		ExternalID:    input.ExternalID,
		CorrelationID: input.CorrelationID,
		CurrentState:  contracts.StateCreated,
		Version:       1,
		FencingToken:  0,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.repo.CreateState(ctx, record); err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "failed to persist initialized state machine record",
			observability.String("saga_id", input.ID),
			observability.String("tenant_id", input.TenantID),
			observability.String("error", err.Error()),
		)
		return nil, err
	}

	s.logger.Info(ctx, "state machine record initialized successfully",
		observability.String("saga_id", record.ID),
		observability.String("tenant_id", record.TenantID),
		observability.String("state", string(record.CurrentState)),
	)

	return record, nil
}

func (s *TransactionStateMachineService) TransitionState(ctx context.Context, input contracts.TransitionStateInput) (*contracts.SagaStateRecord, error) {
	ctx, span := s.tracer.StartSpan(ctx, "state_machine_service.transition")
	defer span.End()

	if err := contracts.ValidateTransitionStateInput(input); err != nil {
		observability.RecordError(span, err)
		s.logger.Warn(ctx, "state transition input validation failed", observability.String("error", err.Error()))
		return nil, err
	}

	currentRecord, err := s.repo.GetState(ctx, input.TenantID, input.SagaID)
	if err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "failed to query current state machine record",
			observability.String("saga_id", input.SagaID),
			observability.String("tenant_id", input.TenantID),
			observability.String("error", err.Error()),
		)
		return nil, err
	}

	if currentRecord.Version != input.ExpectedVersion {
		conflictErr := errors.NewConflictError("stale version detected: reload required")
		observability.RecordError(span, conflictErr)
		s.logger.Warn(ctx, "concurrency conflict: stale state machine version",
			observability.String("saga_id", input.SagaID),
			observability.Int64("current_version", currentRecord.Version),
			observability.Int64("expected_version", input.ExpectedVersion),
		)
		return nil, conflictErr
	}

	if input.FencingToken < currentRecord.FencingToken {
		conflictErr := errors.NewConflictError("stale fencing token: rejected by newer execution owner")
		observability.RecordError(span, conflictErr)
		s.logger.Warn(ctx, "fencing token rejection: stale execution owner",
			observability.String("saga_id", input.SagaID),
			observability.Int64("current_token", currentRecord.FencingToken),
			observability.Int64("submitted_token", input.FencingToken),
		)
		return nil, conflictErr
	}
	nextState, err := s.stateMachine.ComputeNextState(currentRecord.CurrentState, input.Event)
	if err != nil {
		observability.RecordError(span, err)
		s.logger.Warn(ctx, "illegal state machine transition attempted",
			observability.String("current_state", string(currentRecord.CurrentState)),
			observability.String("event", string(input.Event)),
			observability.String("error", err.Error()),
		)
		return nil, err
	}

	updatedRecord, err := s.repo.UpdateStateCAS(
		ctx,
		input.TenantID,
		input.SagaID,
		currentRecord.CurrentState,
		nextState,
		currentRecord.Version,
		input.FencingToken,
		input.Reason,
	)
	if err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "failed CAS update in state machine repository",
			observability.String("saga_id", input.SagaID),
			observability.String("error", err.Error()),
		)
		return nil, err
	}

	s.logger.Info(ctx, "state machine transitioned successfully",
		observability.String("saga_id", input.SagaID),
		observability.String("from_state", string(currentRecord.CurrentState)),
		observability.String("to_state", string(nextState)),
		observability.Int64("version", updatedRecord.Version),
	)

	return updatedRecord, nil
}

func (s *TransactionStateMachineService) GetState(ctx context.Context, tenantID string, sagaID string) (spanResult0 *contracts.SagaStateRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if tenantID == "" || sagaID == "" {
		return nil, errors.NewValidationError("tenantId and sagaId are required")
	}
	return s.repo.GetState(ctx, tenantID, sagaID)
}

func (s *TransactionStateMachineService) GetAuditLogs(ctx context.Context, tenantID string, sagaID string) (spanResult0 []contracts.StateTransitionAuditRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if tenantID == "" || sagaID == "" {
		return nil, errors.NewValidationError("tenantId and sagaId are required")
	}
	return s.repo.GetTransitionAuditLogs(ctx, tenantID, sagaID)
}

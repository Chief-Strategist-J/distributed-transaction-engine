package service

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	machines "distributed-transaction-engine/src/features/transaction-state-machine/domain/machines"
	ports "distributed-transaction-engine/src/features/transaction-state-machine/infrastructure/ports"
	"distributed-transaction-engine/src/shared/errors"
)

type TransactionStateMachineService struct {
	repo         ports.TransactionStateMachineRepositoryPort
	stateMachine *machines.TransactionStateMachine
}

func NewTransactionStateMachineService(
	repo ports.TransactionStateMachineRepositoryPort,
	stateMachine *machines.TransactionStateMachine,
) *TransactionStateMachineService {
	return &TransactionStateMachineService{
		repo:         repo,
		stateMachine: stateMachine,
	}
}

func (s *TransactionStateMachineService) InitializeState(ctx context.Context, input contracts.InitializeStateInput) (*contracts.SagaStateRecord, error) {
	if err := contracts.ValidateInitializeStateInput(input); err != nil {
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
		return nil, err
	}

	return record, nil
}

func (s *TransactionStateMachineService) TransitionState(ctx context.Context, input contracts.TransitionStateInput) (*contracts.SagaStateRecord, error) {
	if err := contracts.ValidateTransitionStateInput(input); err != nil {
		return nil, err
	}

	currentRecord, err := s.repo.GetState(ctx, input.TenantID, input.SagaID)
	if err != nil {
		return nil, err
	}

	if currentRecord.Version != input.ExpectedVersion {
		return nil, errors.NewConflictError("stale version detected: reload required")
	}

	if input.FencingToken < currentRecord.FencingToken {
		return nil, errors.NewConflictError("stale fencing token: rejected by newer execution owner")
	}

	nextState, err := s.stateMachine.ComputeNextState(currentRecord.CurrentState, input.Event)
	if err != nil {
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
		return nil, err
	}

	return updatedRecord, nil
}

func (s *TransactionStateMachineService) GetState(ctx context.Context, tenantID string, sagaID string) (*contracts.SagaStateRecord, error) {
	if tenantID == "" || sagaID == "" {
		return nil, errors.NewValidationError("tenantId and sagaId are required")
	}
	return s.repo.GetState(ctx, tenantID, sagaID)
}

func (s *TransactionStateMachineService) GetAuditLogs(ctx context.Context, tenantID string, sagaID string) ([]contracts.StateTransitionAuditRecord, error) {
	if tenantID == "" || sagaID == "" {
		return nil, errors.NewValidationError("tenantId and sagaId are required")
	}
	return s.repo.GetTransitionAuditLogs(ctx, tenantID, sagaID)
}

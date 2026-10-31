package service

import (
	"context"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaPorts "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	"distributed-transaction-engine/src/shared/errors"
)

type SagaOrchestrationService struct {
	repo sagaPorts.SagaRepositoryPort
}

func NewSagaOrchestrationService(repo sagaPorts.SagaRepositoryPort) *SagaOrchestrationService {
	return &SagaOrchestrationService{
		repo: repo,
	}
}

func (s *SagaOrchestrationService) CreateSaga(ctx context.Context, input sagaContracts.CreateSagaInput) (*sagaContracts.SagaInstance, error) {
	if err := sagaContracts.ValidateCreateSagaInput(input); err != nil {
		return nil, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	instance := &sagaContracts.SagaInstance{
		ID:        input.ID,
		TenantID:  input.TenantID,
		State:     sagaContracts.SagaStatePending,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.repo.CreateSaga(ctx, instance); err != nil {
		return nil, err
	}
	return instance, nil
}

func (s *SagaOrchestrationService) GetSaga(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error) {
	if tenantID == "" || id == "" {
		return nil, errors.NewValidationError("tenantId and id are required")
	}
	return s.repo.GetSagaByID(ctx, tenantID, id)
}

package ports

import (
	"context"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

type SagaRepositoryPort interface {
	CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error
	GetSagaByID(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error)
	GetSagaByPublicID(ctx context.Context, tenantID string, publicID string) (*sagaContracts.SagaInstance, error)
	UpdateSagaState(ctx context.Context, tenantID string, id string, expectedVersion int64, nextState sagaContracts.SagaState) error
	UpdateStepExecution(ctx context.Context, tenantID string, sagaID string, step *sagaContracts.SagaStepExecution) error
	ListSagas(ctx context.Context, tenantID string, limit int, offset int) ([]sagaContracts.SagaInstance, error)
}

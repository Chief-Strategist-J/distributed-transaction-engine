package ports

import (
	"context"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

type SagaRepositoryPort interface {
	GetSagaByID(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error)
	CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error
}

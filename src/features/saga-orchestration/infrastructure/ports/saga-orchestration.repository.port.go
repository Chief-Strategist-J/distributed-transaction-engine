package ports

import (
	"context"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

type SagaRepositoryPort interface {
	CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error
	GetSagaByID(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error)
	GetSagaByPublicID(ctx context.Context, tenantID string, publicID string) (*sagaContracts.SagaInstance, error)
	GetSagaByExternalID(ctx context.Context, tenantID string, workflowDefinitionID string, externalID string) (*sagaContracts.SagaInstance, error)
	UpdateSagaState(ctx context.Context, tenantID string, id string, expectedVersion int64, nextState sagaContracts.SagaState) error
	UpdateSagaStateWithFencing(ctx context.Context, tenantID string, id string, expectedVersion int64, fencingToken int64, nextState sagaContracts.SagaState) error
	AcquireNextLease(ctx context.Context, workerID string, leaseDuration time.Duration) (*sagaContracts.SagaInstance, error)
	AcquireLeaseForSaga(ctx context.Context, tenantID string, id string, workerID string, leaseDuration time.Duration) (*sagaContracts.SagaInstance, error)
	ReleaseLease(ctx context.Context, tenantID string, id string, fencingToken int64) error
	UpdateStepExecution(ctx context.Context, tenantID string, sagaID string, step *sagaContracts.SagaStepExecution) error
	ListSagas(ctx context.Context, tenantID string, limit int, offset int) ([]sagaContracts.SagaInstance, error)
}

package ports

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/crash-recovery/contracts"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

type CrashRecoveryRepositoryPort interface {
	FindExpiredLeases(ctx context.Context, batchSize int) ([]contracts.OrphanCandidate, error)
	TakeoverLease(ctx context.Context, tenantID string, sagaID string, observedToken int64, incarnationID string, leaseDuration time.Duration) (*contracts.TakeoverResult, error)
	MarkPoisoned(ctx context.Context, tenantID string, sagaID string, token int64) error
	RenewHeartbeat(ctx context.Context, tenantID string, sagaID string, incarnationID string, token int64, leaseDuration time.Duration) (bool, error)
	LoadStepExecutions(ctx context.Context, tenantID string, sagaID string) ([]sagaContracts.SagaStepExecution, error)
	UpdateStepExecution(ctx context.Context, tenantID string, sagaID string, step *sagaContracts.SagaStepExecution) error
	ReleaseLease(ctx context.Context, tenantID string, sagaID string, token int64) error
}

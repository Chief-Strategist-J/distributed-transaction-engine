package ports

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"

	"github.com/google/uuid"
)

type RepositoryPort interface {
	ExecuteCAS(ctx context.Context, input contracts.CASMutationInput) (*contracts.CASResult, error)
	DiagnoseState(ctx context.Context, tenantID string, sagaID uuid.UUID) (*contracts.SagaRootState, error)
	ParkPendingInput(ctx context.Context, pending contracts.OCCPendingInput) error
	RecordContention(ctx context.Context, entry contracts.ContentionJournalEntry) error
	IsInputApplied(ctx context.Context, tenantID string, sagaID uuid.UUID, inputID string) (bool, error)
	MarkInputApplied(ctx context.Context, tenantID string, sagaID uuid.UUID, inputID string) error
	GetRecentContentionCount(ctx context.Context, tenantID string, sagaID uuid.UUID, window time.Duration) (int, error)
}


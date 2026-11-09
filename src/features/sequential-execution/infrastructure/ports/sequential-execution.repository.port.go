package ports

import (
	"context"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
)

type SequentialExecutionRepositoryPort interface {
	CreateSequentialSaga(ctx context.Context, saga *contracts.SequentialSagaState) error
	GetSequentialSaga(ctx context.Context, tenantID string, sagaID string) (*contracts.SequentialSagaState, error)
	UpdateSagaCAS(
		ctx context.Context,
		tenantID string,
		sagaID string,
		expectedVersion int64,
		ownerEpoch int64,
		newState string,
		nextStepIndex int,
		nextPhase contracts.ExecutionPhase,
		pivotReached bool,
		output map[string]interface{},
	) error
	SaveContextEntry(ctx context.Context, entry *contracts.StepContextEntry) error
	GetContextEntries(ctx context.Context, tenantID string, sagaID string) ([]contracts.StepContextEntry, error)
}

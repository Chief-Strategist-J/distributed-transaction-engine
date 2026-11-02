package ports

import (
	"context"

	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
)

type TransactionStateMachineRepositoryPort interface {
	GetState(ctx context.Context, tenantID string, sagaID string) (*contracts.SagaStateRecord, error)
	CreateState(ctx context.Context, record *contracts.SagaStateRecord) error
	UpdateStateCAS(
		ctx context.Context,
		tenantID string,
		sagaID string,
		fromState contracts.TransactionState,
		toState contracts.TransactionState,
		expectedVersion int64,
		fencingToken int64,
		reason string,
	) (*contracts.SagaStateRecord, error)
	GetTransitionAuditLogs(ctx context.Context, tenantID string, sagaID string) ([]contracts.StateTransitionAuditRecord, error)
}

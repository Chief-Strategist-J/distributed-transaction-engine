package ports

import (
	"context"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
)

type ParallelExecutionRepositoryPort interface {
	CreateGroupAtomic(ctx context.Context, group *contracts.ParallelGroupInstance, branches []contracts.ParallelBranchInstance) error
	GetGroup(ctx context.Context, tenantID string, groupID string) (*contracts.ParallelGroupInstance, error)
	UpdateBranchOutcome(ctx context.Context, tenantID string, groupID string, branchID string, state contracts.BranchState, output map[string]interface{}, errMsg string) error
	UpdateGroupCAS(ctx context.Context, tenantID string, groupID string, expectedVersion int64, newState contracts.GroupState, succeededCount int, failedCount int) error
}

type ParallelDispatcherPort interface {
	DispatchBranch(ctx context.Context, tenantID string, sagaID string, groupID string, branch contracts.ParallelBranchInstance) error
	CancelBranch(ctx context.Context, tenantID string, sagaID string, groupID string, branchID string) error
	CompensateBranch(ctx context.Context, tenantID string, sagaID string, groupID string, branchID string) error
}

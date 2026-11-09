package adapters

import (
	"context"
	"fmt"
	"sync"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

type MemoryParallelExecutionRepositoryAdapter struct {
	mu     sync.RWMutex
	groups map[string]*contracts.ParallelGroupInstance
}

func NewMemoryParallelExecutionRepositoryAdapter() *MemoryParallelExecutionRepositoryAdapter {
	return &MemoryParallelExecutionRepositoryAdapter{
		groups: make(map[string]*contracts.ParallelGroupInstance),
	}
}

func (m *MemoryParallelExecutionRepositoryAdapter) CreateGroupAtomic(_ context.Context, group *contracts.ParallelGroupInstance, branches []contracts.ParallelBranchInstance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", group.TenantID, group.ID)
	if _, exists := m.groups[key]; exists {
		return errors.NewConflictError("group already exists")
	}

	cp := *group
	cp.Branches = make([]contracts.ParallelBranchInstance, len(branches))
	copy(cp.Branches, branches)
	m.groups[key] = &cp
	return nil
}

func (m *MemoryParallelExecutionRepositoryAdapter) GetGroup(_ context.Context, tenantID string, groupID string) (*contracts.ParallelGroupInstance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, groupID)
	g, exists := m.groups[key]
	if !exists {
		return nil, errors.NewNotFoundError("parallel group not found")
	}
	cp := *g
	return &cp, nil
}

func (m *MemoryParallelExecutionRepositoryAdapter) UpdateBranchOutcome(_ context.Context, tenantID string, groupID string, branchID string, state contracts.BranchState, output map[string]interface{}, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, groupID)
	g, exists := m.groups[key]
	if !exists {
		return errors.NewNotFoundError("group not found")
	}

	for i := range g.Branches {
		if g.Branches[i].BranchID == branchID {
			g.Branches[i].CurrentState = state
			g.Branches[i].BranchOutput = output
			g.Branches[i].ErrorMessage = errMsg
			g.Branches[i].Version++
			return nil
		}
	}
	return errors.NewNotFoundError("branch not found")
}

func (m *MemoryParallelExecutionRepositoryAdapter) UpdateGroupCAS(_ context.Context, tenantID string, groupID string, expectedVersion int64, newState contracts.GroupState, succeededCount int, failedCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, groupID)
	g, exists := m.groups[key]
	if !exists {
		return errors.NewNotFoundError("group not found")
	}
	if g.Version != expectedVersion {
		return errors.NewConflictError("version CAS conflict")
	}

	g.CurrentState = newState
	g.SucceededBranches = succeededCount
	g.FailedBranches = failedCount
	g.Version++
	return nil
}

type MemoryParallelDispatcherAdapter struct {
	mu          sync.Mutex
	Dispatched  []string
	Cancelled   []string
	Compensated []string
}

func NewMemoryParallelDispatcherAdapter() *MemoryParallelDispatcherAdapter {
	return &MemoryParallelDispatcherAdapter{}
}

func (a *MemoryParallelDispatcherAdapter) DispatchBranch(_ context.Context, _ string, _ string, _ string, branch contracts.ParallelBranchInstance) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Dispatched = append(a.Dispatched, branch.BranchID)
	return nil
}

func (a *MemoryParallelDispatcherAdapter) CancelBranch(_ context.Context, _ string, _ string, _ string, branchID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Cancelled = append(a.Cancelled, branchID)
	return nil
}

func (a *MemoryParallelDispatcherAdapter) CompensateBranch(_ context.Context, _ string, _ string, _ string, branchID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Compensated = append(a.Compensated, branchID)
	return nil
}

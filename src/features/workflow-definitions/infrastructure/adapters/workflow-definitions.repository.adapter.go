package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	ports "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"fmt"
	"sync"
)

type MemoryWorkflowDefinitionsRepositoryAdapter struct {
	mu           sync.RWMutex
	byID         map[string]*contracts.WorkflowDefinitionInstance
	byNameVer    map[string]*contracts.WorkflowDefinitionInstance
	byHash       map[string]*contracts.WorkflowDefinitionInstance
	byTenantList map[string][]*contracts.WorkflowDefinitionInstance
}

var _ ports.WorkflowDefinitionsRepositoryPort = (*MemoryWorkflowDefinitionsRepositoryAdapter)(nil)

func NewMemoryWorkflowDefinitionsRepositoryAdapter() *MemoryWorkflowDefinitionsRepositoryAdapter {
	return &MemoryWorkflowDefinitionsRepositoryAdapter{
		byID:         make(map[string]*contracts.WorkflowDefinitionInstance),
		byNameVer:    make(map[string]*contracts.WorkflowDefinitionInstance),
		byHash:       make(map[string]*contracts.WorkflowDefinitionInstance),
		byTenantList: make(map[string][]*contracts.WorkflowDefinitionInstance),
	}
}

func (a *MemoryWorkflowDefinitionsRepositoryAdapter) Save(ctx context.Context, def *contracts.WorkflowDefinitionInstance) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	idKey := fmt.Sprintf("%s:%s", def.TenantID, def.ID)
	nameVerKey := fmt.Sprintf("%s:%s:%d", def.TenantID, def.Name, def.Version)
	hashKey := fmt.Sprintf("%s:%s:%s", def.TenantID, def.Name, def.SpecHash)

	if _, exists := a.byID[idKey]; exists {
		return errors.NewConflictError("workflow definition with id already exists")
	}
	if existing, exists := a.byNameVer[nameVerKey]; exists {
		if existing.SpecHash != def.SpecHash {
			return errors.NewConflictError(fmt.Sprintf("workflow %s version %d already exists with different spec hash", def.Name, def.Version))
		}
		return errors.NewConflictError(fmt.Sprintf("workflow %s version %d already exists", def.Name, def.Version))
	}
	if _, exists := a.byHash[hashKey]; exists {
		return errors.NewConflictError("identical workflow spec already registered under a different version")
	}

	copied := *def
	stepsCopy := make([]contracts.StepDefinition, len(def.Steps))
	copy(stepsCopy, def.Steps)
	copied.Steps = stepsCopy

	a.byID[idKey] = &copied
	a.byNameVer[nameVerKey] = &copied
	a.byHash[hashKey] = &copied
	a.byTenantList[def.TenantID] = append(a.byTenantList[def.TenantID], &copied)

	return nil
}

func (a *MemoryWorkflowDefinitionsRepositoryAdapter) GetByID(ctx context.Context, tenantID string, id string) (spanResult0 *contracts.WorkflowDefinitionInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, id)
	item, ok := a.byID[key]
	if !ok {
		return nil, errors.NewNotFoundError(fmt.Sprintf("workflow definition %s not found for tenant %s", id, tenantID))
	}
	return item, nil
}

func (a *MemoryWorkflowDefinitionsRepositoryAdapter) GetByNameAndVersion(ctx context.Context, tenantID string, name string, version int64) (spanResult0 *contracts.WorkflowDefinitionInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%d", tenantID, name, version)
	item, ok := a.byNameVer[key]
	if !ok {
		return nil, errors.NewNotFoundError(fmt.Sprintf("workflow definition %s version %d not found for tenant %s", name, version, tenantID))
	}
	return item, nil
}

func (a *MemoryWorkflowDefinitionsRepositoryAdapter) GetBySpecHash(ctx context.Context, tenantID string, name string, hash string) (spanResult0 *contracts.WorkflowDefinitionInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, name, hash)
	item, ok := a.byHash[key]
	if !ok {
		return nil, errors.NewNotFoundError(fmt.Sprintf("workflow definition with specified hash not found for tenant %s", tenantID))
	}
	return item, nil
}

func (a *MemoryWorkflowDefinitionsRepositoryAdapter) ListByTenant(ctx context.Context, tenantID string, limit int, offset int) (spanResult0 []*contracts.WorkflowDefinitionInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	list, ok := a.byTenantList[tenantID]
	if !ok {
		return []*contracts.WorkflowDefinitionInstance{}, nil
	}

	if offset >= len(list) {
		return []*contracts.WorkflowDefinitionInstance{}, nil
	}

	end := offset + limit
	if end > len(list) {
		end = len(list)
	}

	return list[offset:end], nil
}

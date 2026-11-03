package ports

import (
	"context"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
)

type WorkflowDefinitionsRepositoryPort interface {
	Save(ctx context.Context, def *contracts.WorkflowDefinitionInstance) error
	GetByID(ctx context.Context, tenantID string, id string) (*contracts.WorkflowDefinitionInstance, error)
	GetByNameAndVersion(ctx context.Context, tenantID string, name string, version int64) (*contracts.WorkflowDefinitionInstance, error)
	GetBySpecHash(ctx context.Context, tenantID string, name string, hash string) (*contracts.WorkflowDefinitionInstance, error)
	ListByTenant(ctx context.Context, tenantID string, limit int, offset int) ([]*contracts.WorkflowDefinitionInstance, error)
}


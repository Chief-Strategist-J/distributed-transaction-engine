package workflowdefinitions

import (
	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	adapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	service "distributed-transaction-engine/src/features/workflow-definitions/service"
)

type Service = service.WorkflowDefinitionsService
type StepDefinition = contracts.StepDefinition
type WorkflowDefinitionInstance = contracts.WorkflowDefinitionInstance
type CreateWorkflowDefinitionInput = contracts.CreateWorkflowDefinitionInput
type RepositoryPort = ports.WorkflowDefinitionsRepositoryPort
type MemoryAdapter = adapters.MemoryWorkflowDefinitionsRepositoryAdapter
type PostgresAdapter = adapters.PostgresWorkflowDefinitionsRepositoryAdapter

func NewService(repo RepositoryPort) *Service {
	return service.NewWorkflowDefinitionsService(repo)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
}


package sagaorchestration

import (
	contracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	adapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	service "distributed-transaction-engine/src/features/saga-orchestration/service"
	workflowPorts "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
)

type Service = service.SagaOrchestrationService
type Instance = contracts.SagaInstance
type State = contracts.SagaState
type StepExecution = contracts.SagaStepExecution
type StepState = contracts.StepExecutionState
type ExecuteInput = contracts.ExecuteSagaInput
type RepositoryPort = ports.SagaRepositoryPort
type StepDispatcherPort = ports.StepDispatcherPort
type MemoryAdapter = adapters.MemorySagaRepositoryAdapter
type PostgresAdapter = adapters.PostgresSagaRepositoryAdapter
type HttpDispatcher = adapters.HttpStepDispatcherAdapter

func NewService(repo RepositoryPort, workflowRepo workflowPorts.WorkflowDefinitionsRepositoryPort, dispatcher StepDispatcherPort) *Service {
	return service.NewSagaOrchestrationService(repo, workflowRepo, dispatcher)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemorySagaRepositoryAdapter()
}

func NewHttpDispatcher() *HttpDispatcher {
	return adapters.NewHttpStepDispatcherAdapter()
}

package compensationengine

import (
	"database/sql"

	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	machines "distributed-transaction-engine/src/features/compensation-engine/domain/machines"
	adapters "distributed-transaction-engine/src/features/compensation-engine/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/compensation-engine/infrastructure/ports"
	service "distributed-transaction-engine/src/features/compensation-engine/service"
)

type Service = service.CompensationService
type ServiceDeps = service.CompensationServiceDeps
type PlanInstance = contracts.SagaCompensationPlanInstance
type StepInstance = contracts.SagaStepCompensationInstance
type PlanState = contracts.CompensationPlanState
type StepStatus = contracts.StepCompensationStatus
type TriggerInput = contracts.TriggerCompensationInput
type CompleteStepInput = contracts.CompleteStepCompensationInput
type RepositoryPort = ports.CompensationRepositoryPort
type DispatcherPort = ports.CompensationDispatcherPort
type MemoryAdapter = adapters.MemoryCompensationRepositoryAdapter
type PostgresAdapter = adapters.PostgresCompensationRepositoryAdapter
type MemoryDispatcher = adapters.MemoryCompensationDispatcherAdapter
type StateMachine = machines.CompensationStateMachine

func NewService(deps ServiceDeps) *Service {
	return service.NewCompensationService(deps)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemoryCompensationRepositoryAdapter()
}

func NewPostgresAdapter(db *sql.DB) *PostgresAdapter {
	return adapters.NewPostgresCompensationRepositoryAdapter(db)
}

func NewMemoryDispatcher() *MemoryDispatcher {
	return adapters.NewMemoryCompensationDispatcherAdapter()
}

func NewStateMachine() *StateMachine {
	return machines.NewCompensationStateMachine()
}

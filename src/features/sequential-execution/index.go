package sequentialexecution

import (
	"database/sql"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	adapters "distributed-transaction-engine/src/features/sequential-execution/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/sequential-execution/infrastructure/ports"
	service "distributed-transaction-engine/src/features/sequential-execution/service"
)

type Service = service.SequentialExecutionService
type ServiceDeps = service.SequentialExecutionServiceDeps
type State = contracts.SequentialSagaState
type Phase = contracts.ExecutionPhase
type StartInput = contracts.StartSequentialSagaInput
type RepositoryPort = ports.SequentialExecutionRepositoryPort
type DispatcherPort = ports.SequentialDispatcherPort
type MemoryAdapter = adapters.MemorySequentialExecutionRepositoryAdapter
type PostgresAdapter = adapters.PostgresSequentialExecutionRepositoryAdapter
type MemoryDispatcher = adapters.MemorySequentialDispatcherAdapter

func NewService(deps ServiceDeps) *Service {
	return service.NewSequentialExecutionService(deps)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemorySequentialExecutionRepositoryAdapter()
}

func NewMemoryDispatcher() *MemoryDispatcher {
	return adapters.NewMemorySequentialDispatcherAdapter()
}

func NewPostgresAdapter(db *sql.DB) *PostgresAdapter {
	return adapters.NewPostgresSequentialExecutionRepositoryAdapter(db)
}

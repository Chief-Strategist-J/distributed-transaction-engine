package parallelexecution

import (
	"database/sql"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	adapters "distributed-transaction-engine/src/features/parallel-execution/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/parallel-execution/infrastructure/ports"
	service "distributed-transaction-engine/src/features/parallel-execution/service"
)

type Service = service.ParallelExecutionService
type ServiceDeps = service.ParallelExecutionServiceDeps
type GroupInstance = contracts.ParallelGroupInstance
type BranchInstance = contracts.ParallelBranchInstance
type JoinPolicy = contracts.JoinPolicy
type StartInput = contracts.StartParallelGroupInput
type RepositoryPort = ports.ParallelExecutionRepositoryPort
type DispatcherPort = ports.ParallelDispatcherPort
type MemoryAdapter = adapters.MemoryParallelExecutionRepositoryAdapter
type PostgresAdapter = adapters.PostgresParallelExecutionRepositoryAdapter
type MemoryDispatcher = adapters.MemoryParallelDispatcherAdapter

func NewService(deps ServiceDeps) *Service {
	return service.NewParallelExecutionService(deps)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemoryParallelExecutionRepositoryAdapter()
}

func NewMemoryDispatcher() *MemoryDispatcher {
	return adapters.NewMemoryParallelDispatcherAdapter()
}

func NewPostgresAdapter(db *sql.DB) *PostgresAdapter {
	return adapters.NewPostgresParallelExecutionRepositoryAdapter(db)
}

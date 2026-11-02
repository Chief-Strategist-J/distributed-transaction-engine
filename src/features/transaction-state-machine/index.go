package transactionstatemachine

import (
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	machines "distributed-transaction-engine/src/features/transaction-state-machine/domain/machines"
	adapters "distributed-transaction-engine/src/features/transaction-state-machine/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/transaction-state-machine/infrastructure/ports"
	service "distributed-transaction-engine/src/features/transaction-state-machine/service"
)

type Service = service.TransactionStateMachineService
type State = contracts.TransactionState
type Event = contracts.StateTransitionEvent
type StateRecord = contracts.SagaStateRecord
type AuditRecord = contracts.StateTransitionAuditRecord
type TransitionInput = contracts.TransitionStateInput
type InitializeInput = contracts.InitializeStateInput
type RepositoryPort = ports.TransactionStateMachineRepositoryPort
type MemoryAdapter = adapters.MemoryTransactionStateMachineRepositoryAdapter
type PostgresAdapter = adapters.PostgresTransactionStateMachineRepositoryAdapter
type Machine = machines.TransactionStateMachine

func NewService(repo ports.TransactionStateMachineRepositoryPort) *Service {
	machine := machines.NewTransactionStateMachine()
	return service.NewTransactionStateMachineService(repo, machine)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemoryTransactionStateMachineRepositoryAdapter()
}

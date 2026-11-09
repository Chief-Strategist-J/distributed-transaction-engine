package compensationretry

import (
	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"distributed-transaction-engine/src/features/compensation-retry/domain/machines"
	"distributed-transaction-engine/src/features/compensation-retry/domain/rules"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/adapters"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/ports"
	"distributed-transaction-engine/src/features/compensation-retry/service"
)

type (
	RepositoryPort                  = ports.RepositoryPort
	CompensationRetryService        = service.CompensationRetryService
	ServiceDeps                     = service.ServiceDeps
	CompensationRetryRecord         = contracts.CompensationRetryRecord
	EvaluateCompensationRetryInput   = contracts.EvaluateCompensationRetryInput
	CompensationRetryDecision       = contracts.CompensationRetryDecision
	OutcomeClass                    = contracts.OutcomeClass
	EscalationLevel                 = contracts.EscalationLevel
	CompensationRetryRules          = rules.CompensationRetryRules
	CompensationRetryMachine        = machines.CompensationRetryMachine
	CompensationState               = machines.CompensationState
	CompensationEvent               = machines.CompensationEvent
)

func NewService(deps ServiceDeps) *CompensationRetryService {
	return service.NewService(deps)
}

func NewMemoryAdapter() *adapters.MemoryCompensationRetryRepositoryAdapter {
	return adapters.NewMemoryCompensationRetryRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.DBExecutor) *adapters.PostgresCompensationRetryRepositoryAdapter {
	return adapters.NewPostgresCompensationRetryRepositoryAdapter(db)
}

func NewCompensationRetryRules() *CompensationRetryRules {
	return rules.NewCompensationRetryRules()
}

func NewCompensationRetryMachine() *CompensationRetryMachine {
	return machines.NewCompensationRetryMachine()
}


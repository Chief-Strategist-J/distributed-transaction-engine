package manualintervention

import (
	"distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/features/manual-intervention/domain/machines"
	"distributed-transaction-engine/src/features/manual-intervention/domain/rules"
	"distributed-transaction-engine/src/features/manual-intervention/infrastructure/adapters"
	"distributed-transaction-engine/src/features/manual-intervention/infrastructure/ports"
	"distributed-transaction-engine/src/features/manual-intervention/service"
)

type (
	RepositoryPort                = ports.ManualInterventionRepositoryPort
	ManualInterventionService     = service.ManualInterventionService
	ServiceDeps                   = service.ServiceDeps
	ManualInterventionAuditRecord = contracts.ManualInterventionAuditRecord
	ExecuteInterventionInput      = contracts.ExecuteInterventionInput
	InterventionExecutionResult   = contracts.InterventionExecutionResult
	InterventionActionType        = contracts.InterventionActionType
	ManualInterventionRules       = rules.ManualInterventionRules
	ManualInterventionMachine     = machines.ManualInterventionMachine
)

func NewService(deps ServiceDeps) *ManualInterventionService {
	return service.NewService(deps)
}

func NewMemoryAdapter() *adapters.MemoryManualInterventionRepositoryAdapter {
	return adapters.NewMemoryManualInterventionRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.DBExecutor) *adapters.PostgresManualInterventionRepositoryAdapter {
	return adapters.NewPostgresManualInterventionRepositoryAdapter(db)
}

func NewManualInterventionRules() *ManualInterventionRules {
	return rules.NewManualInterventionRules()
}

func NewManualInterventionMachine() *ManualInterventionMachine {
	return machines.NewManualInterventionMachine()
}

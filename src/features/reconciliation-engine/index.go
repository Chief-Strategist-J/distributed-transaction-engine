package reconciliationengine

import (
	"distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/machines"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/rules"
	"distributed-transaction-engine/src/features/reconciliation-engine/infrastructure/adapters"
	"distributed-transaction-engine/src/features/reconciliation-engine/infrastructure/ports"
	"distributed-transaction-engine/src/features/reconciliation-engine/service"
)

type (
	RepositoryPort                 = ports.ReconciliationRepositoryPort
	ReconciliationService          = service.ReconciliationService
	ServiceDeps                    = service.ServiceDeps
	ReconciliationAnomalyRecord    = contracts.ReconciliationAnomalyRecord
	EvaluateReconciliationInput    = contracts.EvaluateReconciliationInput
	ReconciliationEvaluationResult = contracts.ReconciliationEvaluationResult
	AnomalyType                    = contracts.AnomalyType
	ReconciliationStatus           = contracts.ReconciliationStatus
	ReconciliationRules            = rules.ReconciliationRules
	ReconciliationMachine          = machines.ReconciliationMachine
)

func NewService(deps ServiceDeps) *ReconciliationService {
	return service.NewService(deps)
}

func NewMemoryAdapter() *adapters.MemoryReconciliationRepositoryAdapter {
	return adapters.NewMemoryReconciliationRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.DBExecutor) *adapters.PostgresReconciliationRepositoryAdapter {
	return adapters.NewPostgresReconciliationRepositoryAdapter(db)
}

func NewReconciliationRules() *ReconciliationRules {
	return rules.NewReconciliationRules()
}

func NewReconciliationMachine() *ReconciliationMachine {
	return machines.NewReconciliationMachine()
}

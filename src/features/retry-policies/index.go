package retrypolicies

import (
	"distributed-transaction-engine/src/features/retry-policies/contracts"
	"distributed-transaction-engine/src/features/retry-policies/domain/machines"
	"distributed-transaction-engine/src/features/retry-policies/infrastructure/adapters"
	"distributed-transaction-engine/src/features/retry-policies/infrastructure/ports"
	"distributed-transaction-engine/src/features/retry-policies/service"
)

type (
	ErrorClass             = contracts.ErrorClass
	BackoffAlgorithm       = contracts.BackoffAlgorithm
	SagaPosition           = contracts.SagaPosition
	StepCapability         = contracts.StepCapability
	RetryPolicyConfig      = contracts.RetryPolicyConfig
	RetryDecisionAction    = contracts.RetryDecisionAction
	RetryDecision          = contracts.RetryDecision
	EvaluateRetryInput     = contracts.EvaluateRetryInput
	BreakerState           = machines.BreakerState
	CircuitBreakerManager  = machines.CircuitBreakerManager
	RepositoryPort         = ports.RepositoryPort
	ServiceDeps            = service.ServiceDeps
	RetryPolicyService     = service.RetryPolicyService
	DBExecutor             = adapters.DBExecutor
)

func NewService(deps ServiceDeps) *RetryPolicyService {
	return service.NewService(deps)
}

func NewCircuitBreakerManager() *CircuitBreakerManager {
	return machines.NewCircuitBreakerManager()
}

func NewMemoryAdapter() RepositoryPort {
	return adapters.NewMemoryRetryRepositoryAdapter()
}

func NewPostgresAdapter(db DBExecutor) RepositoryPort {
	return adapters.NewPostgresRetryRepositoryAdapter(db)
}

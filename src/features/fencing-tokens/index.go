package fencingtokens

import (
	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/machines"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/rules"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/adapters"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/ports"
	"distributed-transaction-engine/src/features/fencing-tokens/service"
)

type (
	RepositoryPort        = ports.RepositoryPort
	FencingTokensService  = service.FencingTokensService
	ServiceDeps           = service.ServiceDeps
	FencingLease          = contracts.FencingLease
	FencingAudit          = contracts.FencingAudit
	AcquireLeaseInput     = contracts.AcquireLeaseInput
	RenewLeaseInput       = contracts.RenewLeaseInput
	ReleaseLeaseInput     = contracts.ReleaseLeaseInput
	FencedBarrierInput    = contracts.FencedBarrierInput
	FencingDecision       = contracts.FencingDecision
	FencingRules          = rules.FencingRules
	FencingLeaseMachine   = machines.FencingLeaseMachine
	LeaseState            = machines.LeaseState
	LeaseEvent            = machines.LeaseEvent
)

func NewService(deps ServiceDeps) *FencingTokensService {
	return service.NewService(deps)
}

func NewMemoryAdapter() *adapters.MemoryFencingRepositoryAdapter {
	return adapters.NewMemoryFencingRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.DBExecutor) *adapters.PostgresFencingRepositoryAdapter {
	return adapters.NewPostgresFencingRepositoryAdapter(db)
}

func NewFencingRules() *FencingRules {
	return rules.NewFencingRules()
}

func NewFencingLeaseMachine() *FencingLeaseMachine {
	return machines.NewFencingLeaseMachine()
}


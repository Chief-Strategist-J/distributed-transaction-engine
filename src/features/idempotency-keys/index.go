package idempotencykeys

import (
	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/features/idempotency-keys/infrastructure/adapters"
	"distributed-transaction-engine/src/features/idempotency-keys/infrastructure/ports"
	"distributed-transaction-engine/src/features/idempotency-keys/service"
)

type (
	IdempotencyState       = contracts.IdempotencyState
	IdempotencyRecord      = contracts.IdempotencyRecord
	ResponseSnapshot       = contracts.ResponseSnapshot
	AcquireOutcome         = contracts.AcquireOutcome
	AcquireResult          = contracts.AcquireResult
	IngestIdempotencyInput = contracts.IngestIdempotencyInput
	RepositoryPort         = ports.RepositoryPort
	ServiceDeps            = service.ServiceDeps
	IdempotencyService     = service.IdempotencyService
)

func NewService(deps ServiceDeps) *IdempotencyService {
	return service.NewService(deps)
}

func NewMemoryAdapter() RepositoryPort {
	return adapters.NewMemoryIdempotencyRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.DBExecutor) RepositoryPort {
	return adapters.NewPostgresIdempotencyRepositoryAdapter(db)
}

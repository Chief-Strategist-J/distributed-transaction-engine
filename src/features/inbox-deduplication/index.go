package inboxdeduplication

import (
	"time"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	adapters "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/ports"
	service "distributed-transaction-engine/src/features/inbox-deduplication/service"
)

type Service = service.InboxDeduplicationService
type ServiceDeps = service.InboxDeduplicationServiceDeps
type IngestInput = contracts.IngestMessageInput
type IngestDecision = contracts.IngestDecision
type Message = contracts.InboxMessage
type Status = contracts.InboxMessageStatus
type Outcome = contracts.IngestOutcome
type RepositoryPort = ports.InboxDeduplicationRepositoryPort
type MemoryAdapter = adapters.MemoryInboxDeduplicationRepositoryAdapter
type PostgresAdapter = adapters.PostgresInboxDeduplicationRepositoryAdapter
type Pruner = service.InboxPruner

func NewService(deps ServiceDeps) *Service {
	return service.NewInboxDeduplicationService(deps)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemoryInboxDeduplicationRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.PostgresInboxDeduplicationRepositoryAdapter) *PostgresAdapter {
	return &db
}

func NewPruner(svc *Service, retention time.Duration, interval time.Duration, batchSize int) *Pruner {
	return service.NewInboxPruner(svc, retention, interval, batchSize)
}

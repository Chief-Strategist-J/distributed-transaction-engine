package transactionaloutbox

import (
	"database/sql"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	adapters "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/ports"
	service "distributed-transaction-engine/src/features/transactional-outbox/service"
)

type Service = service.OutboxService
type ServiceDeps = service.OutboxServiceDeps
type EventInstance = contracts.OutboxEventInstance
type Status = contracts.OutboxStatus
type RepositoryPort = ports.OutboxRepositoryPort
type PublisherPort = ports.KafkaPublisherPort
type MemoryAdapter = adapters.MemoryOutboxRepositoryAdapter
type PostgresAdapter = adapters.PostgresOutboxRepositoryAdapter
type KafkaPublisher = adapters.KafkaPublisherAdapter
type MemoryPublisher = adapters.MemoryKafkaPublisherAdapter
type StateMachine = machines.OutboxStateMachine
type Relay = service.OutboxRelay
type KafkaPublisherOptions = adapters.KafkaPublisherOptions

func NewService(deps ServiceDeps) *Service {
	return service.NewOutboxService(deps)
}

func NewMemoryAdapter() *MemoryAdapter {
	return adapters.NewMemoryOutboxRepositoryAdapter()
}

func NewPostgresAdapter(db *sql.DB) *PostgresAdapter {
	return adapters.NewPostgresOutboxRepositoryAdapter(db)
}

func NewKafkaPublisher(brokerAddress string) *KafkaPublisher {
	return adapters.NewKafkaPublisherAdapter(brokerAddress)
}

func NewMemoryPublisher() *MemoryPublisher {
	return adapters.NewMemoryKafkaPublisherAdapter()
}

func NewStateMachine() *StateMachine {
	return machines.NewOutboxStateMachine()
}

func NewKafkaPublisherWithOptions(brokerAddress string, options KafkaPublisherOptions) *KafkaPublisher {
	return adapters.NewKafkaPublisherAdapterWithOptions(brokerAddress, options)
}

func NewRelay(svc *Service, workerID string, batchLimit int, leaseDuration time.Duration, interval time.Duration) *Relay {
	return service.NewOutboxRelay(svc, workerID, batchLimit, leaseDuration, interval)
}

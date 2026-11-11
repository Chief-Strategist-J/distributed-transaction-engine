package ports

import (
	"context"
	"database/sql"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
)

type OutboxRepositoryPort interface {
	InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event *contracts.OutboxEventInstance) error
	ClaimPendingEvents(ctx context.Context, input *contracts.ClaimOutboxEventsInput) ([]contracts.OutboxEventInstance, error)
	ConfirmPublished(ctx context.Context, input *contracts.ConfirmOutboxPublishInput) error
	ConfirmPublishedBatch(ctx context.Context, tenantID string, workerID string, eventIDs []string) (int, error)
	ReleaseInFlightToPending(ctx context.Context, tenantID string, eventID string, reason string) error
	ParkEvent(ctx context.Context, input *contracts.ParkOutboxEventInput) error
	GetEvent(ctx context.Context, tenantID string, eventID string) (*contracts.OutboxEventInstance, error)
	ListRelayTargets(ctx context.Context, limit int) ([]contracts.OutboxRelayTarget, error)
}

type KafkaPublisherPort interface {
	PublishMessage(ctx context.Context, topic string, partitionKey string, headers map[string]interface{}, payload []byte) error
	PublishBatch(ctx context.Context, requests []contracts.PublishRequest) []error
}

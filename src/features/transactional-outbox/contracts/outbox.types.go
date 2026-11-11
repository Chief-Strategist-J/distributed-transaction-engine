package contracts

import (
	"time"
)

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusInFlight  OutboxStatus = "IN_FLIGHT"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusParked    OutboxStatus = "PARKED"
)

type OutboxEventInstance struct {
	ID                string                 `json:"id"`
	PublicID          string                 `json:"publicId"`
	TenantID          string                 `json:"tenantId"`
	SagaID            string                 `json:"sagaId"`
	AggregateType     string                 `json:"aggregateType"`
	AggregateID       string                 `json:"aggregateId"`
	AggregateSequence int64                  `json:"aggregateSequence"`
	DestinationTopic  string                 `json:"destinationTopic"`
	PartitionKey      string                 `json:"partitionKey"`
	EventType         string                 `json:"eventType"`
	Payload           map[string]interface{} `json:"payload"`
	Headers           map[string]interface{} `json:"headers"`
	Status            OutboxStatus           `json:"status"`
	LeaseOwner        string                 `json:"leaseOwner,omitempty"`
	LeaseExpiresAt    *time.Time             `json:"leaseExpiresAt,omitempty"`
	NextAttemptAt     *time.Time             `json:"nextAttemptAt,omitempty"`
	AttemptCount      int                    `json:"attemptCount"`
	LastError         string                 `json:"lastError,omitempty"`
	Version           int64                  `json:"version"`
	CreatedAt         time.Time              `json:"createdAt"`
	UpdatedAt         time.Time              `json:"updatedAt"`
	PublishedAt       *time.Time             `json:"publishedAt,omitempty"`
}

type CreateOutboxEventInput struct {
	ID                string                 `json:"id"`
	PublicID          string                 `json:"publicId"`
	TenantID          string                 `json:"tenantId"`
	SagaID            string                 `json:"sagaId"`
	AggregateType     string                 `json:"aggregateType"`
	AggregateID       string                 `json:"aggregateId"`
	AggregateSequence int64                  `json:"aggregateSequence"`
	DestinationTopic  string                 `json:"destinationTopic"`
	PartitionKey      string                 `json:"partitionKey"`
	EventType         string                 `json:"eventType"`
	Payload           map[string]interface{} `json:"payload"`
	Headers           map[string]interface{} `json:"headers"`
}

type ClaimOutboxEventsInput struct {
	TenantID      string        `json:"tenantId"`
	Topic         string        `json:"topic"`
	WorkerID      string        `json:"workerId"`
	LeaseDuration time.Duration `json:"leaseDuration"`
	BatchLimit    int           `json:"batchLimit"`
}

type ConfirmOutboxPublishInput struct {
	TenantID string `json:"tenantId"`
	EventID  string `json:"eventId"`
	WorkerID string `json:"workerId"`
}

type ParkOutboxEventInput struct {
	TenantID string `json:"tenantId"`
	EventID  string `json:"eventId"`
	WorkerID string `json:"workerId"`
	Reason   string `json:"reason"`
}

type OutboxRelayTarget struct {
	TenantID string
	Topic    string
}

type PublishRequest struct {
	Topic        string
	PartitionKey string
	Headers      map[string]interface{}
	Payload      []byte
}

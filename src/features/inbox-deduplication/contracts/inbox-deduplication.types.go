package contracts

import (
	"time"
)

type InboxMessageStatus string

const (
	StatusApplied     InboxMessageStatus = "APPLIED"
	StatusHeld        InboxMessageStatus = "HELD"
	StatusQuarantined InboxMessageStatus = "QUARANTINED"
)

type IngestOutcome string

const (
	OutcomeApplied           IngestOutcome = "APPLIED"
	OutcomeDuplicate         IngestOutcome = "DUPLICATE"
	OutcomeIntegrityMismatch IngestOutcome = "INTEGRITY_MISMATCH"
)

type IngestMessageInput struct {
	ID                 string                 `json:"id"`
	PublicID           string                 `json:"publicId"`
	TenantID           string                 `json:"tenantId"`
	Source             string                 `json:"source"`
	ExternalID         string                 `json:"externalId"`
	CorrelationID      string                 `json:"correlationId"`
	HandlerName        string                 `json:"handlerName"`
	HandlerGeneration  int                    `json:"handlerGeneration"`
	ProducerEventTime  time.Time              `json:"producerEventTime"`
	Payload            map[string]interface{} `json:"payload"`
}

type InboxMessage struct {
	ID                string                 `json:"id"`
	PublicID          string                 `json:"publicId"`
	TenantID          string                 `json:"tenantId"`
	Source            string                 `json:"source"`
	ExternalID        string                 `json:"externalId"`
	CorrelationID     string                 `json:"correlationId"`
	HandlerName       string                 `json:"handlerName"`
	HandlerGeneration int                    `json:"handlerGeneration"`
	ContentHash       string                 `json:"contentHash"`
	Status            InboxMessageStatus     `json:"status"`
	AttemptCount      int                    `json:"attemptCount"`
	ProducerEventTime time.Time              `json:"producerEventTime"`
	Payload           map[string]interface{} `json:"payload"`
	Version           int64                  `json:"version"`
	CreatedAt         time.Time              `json:"createdAt"`
	UpdatedAt         time.Time              `json:"updatedAt"`
}

type IngestDecision struct {
	Outcome     IngestOutcome
	StoredHash  string
	IncomingHash string
}


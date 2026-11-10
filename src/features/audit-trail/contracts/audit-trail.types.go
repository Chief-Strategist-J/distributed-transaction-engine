package contracts

import (
	"time"

	"github.com/google/uuid"
)

type OutboxStatus string

const (
	OutboxStatusPending OutboxStatus = "PENDING"
	OutboxStatusChained OutboxStatus = "CHAINED"
	OutboxStatusFailed  OutboxStatus = "FAILED"
)

type QuarantineReason string

const (
	QuarantineReasonAttributionMissing QuarantineReason = "ATTRIBUTION_MISSING"
	QuarantineReasonSchemaInvalid      QuarantineReason = "SCHEMA_INVALID"
	QuarantineReasonPayloadCorrupt     QuarantineReason = "PAYLOAD_CORRUPT"
)

type VerificationStatus string

const (
	VerificationStatusValid       VerificationStatus = "VALID"
	VerificationStatusCompromised VerificationStatus = "COMPROMISED"
)

type OutboxEventRecord struct {
	ID             uuid.UUID              `json:"id"`
	PublicID       string                 `json:"public_id"`
	TenantID       string                 `json:"tenant_id"`
	Source         string                 `json:"source"`
	ExternalID     string                 `json:"external_id"`
	CorrelationID  uuid.UUID              `json:"correlation_id"`
	AuditID        string                 `json:"audit_id"`
	OccurredAt     time.Time              `json:"occurred_at"`
	Action         string                 `json:"action"`
	ActorID        string                 `json:"actor_id"`
	ActorIP        string                 `json:"actor_ip"`
	OnBehalfOf     string                 `json:"on_behalf_of"`
	TargetResource string                 `json:"target_resource"`
	Payload        map[string]interface{} `json:"payload"`
	Status         OutboxStatus           `json:"status"`
	CreatedAt      time.Time              `json:"created_at"`
	ChainedAt      *time.Time             `json:"chained_at,omitempty"`
}

type AuditEventRecord struct {
	ID            uuid.UUID              `json:"id"`
	PublicID      string                 `json:"public_id"`
	TenantID      string                 `json:"tenant_id"`
	Source        string                 `json:"source"`
	ExternalID    string                 `json:"external_id"`
	CorrelationID uuid.UUID              `json:"correlation_id"`
	AuditID       string                 `json:"audit_id"`
	SegmentID     uuid.UUID              `json:"segment_id"`
	SequenceID    int64                  `json:"sequence_id"`
	KeyID         string                 `json:"key_id"`
	ActorID       string                 `json:"actor_id"`
	ActorIP       string                 `json:"actor_ip"`
	Action        string                 `json:"action"`
	PrevHash      string                 `json:"prev_hash"`
	CurrHash      string                 `json:"curr_hash"`
	RawPayload    map[string]interface{} `json:"raw_payload"`
	OccurredAt    time.Time              `json:"occurred_at"`
	CreatedAt     time.Time              `json:"created_at"`
}

type AuditQuarantineRecord struct {
	ID               uuid.UUID              `json:"id"`
	PublicID         string                 `json:"public_id"`
	TenantID         string                 `json:"tenant_id"`
	Source           string                 `json:"source"`
	ExternalID       string                 `json:"external_id"`
	CorrelationID    uuid.UUID              `json:"correlation_id"`
	AuditID          string                 `json:"audit_id"`
	QuarantineReason QuarantineReason       `json:"quarantine_reason"`
	RawRecord        map[string]interface{} `json:"raw_record"`
	ErrorDetails     string                 `json:"error_details"`
	Resolved         bool                   `json:"resolved"`
	CreatedAt        time.Time              `json:"created_at"`
}

type AuditCheckpointRecord struct {
	ID             uuid.UUID `json:"id"`
	PublicID       string    `json:"public_id"`
	TenantID       string    `json:"tenant_id"`
	Source         string    `json:"source"`
	ExternalID     string    `json:"external_id"`
	CorrelationID  uuid.UUID `json:"correlation_id"`
	SegmentID      uuid.UUID `json:"segment_id"`
	LastSequenceID int64     `json:"last_sequence_id"`
	HeadHash       string    `json:"head_hash"`
	Signature      string    `json:"signature"`
	KeyID          string    `json:"key_id"`
	EventCount     int       `json:"event_count"`
	CreatedAt      time.Time `json:"created_at"`
}

type AuditVerificationRecord struct {
	ID                   uuid.UUID          `json:"id"`
	PublicID             string             `json:"public_id"`
	TenantID             string             `json:"tenant_id"`
	Source               string             `json:"source"`
	ExternalID           string             `json:"external_id"`
	CorrelationID        uuid.UUID          `json:"correlation_id"`
	SegmentID            uuid.UUID          `json:"segment_id"`
	VerifiedUpToSequence int64              `json:"verified_up_to_sequence"`
	TotalEventsChecked   int                `json:"total_events_checked"`
	Status               VerificationStatus `json:"status"`
	FailureSequenceID    *int64             `json:"failure_sequence_id,omitempty"`
	FailureReason        string             `json:"failure_reason"`
	VerifiedBy           string             `json:"verified_by"`
	CreatedAt            time.Time          `json:"created_at"`
}

type CaptureOutboxEventCommand struct {
	TenantID       string                 `json:"tenant_id"`
	Source         string                 `json:"source"`
	ExternalID     string                 `json:"external_id"`
	CorrelationID  uuid.UUID              `json:"correlation_id"`
	AuditID        string                 `json:"audit_id"`
	OccurredAt     time.Time              `json:"occurred_at"`
	Action         string                 `json:"action"`
	ActorID        string                 `json:"actor_id"`
	ActorIP        string                 `json:"actor_ip"`
	OnBehalfOf     string                 `json:"on_behalf_of"`
	TargetResource string                 `json:"target_resource"`
	Payload        map[string]interface{} `json:"payload"`
}

type ProcessChainerBatchCommand struct {
	TenantID  string    `json:"tenant_id"`
	SegmentID uuid.UUID `json:"segment_id"`
	KeyID     string    `json:"key_id"`
	SecretKey []byte    `json:"-"`
	BatchSize int       `json:"batch_size"`
}

type ChainerBatchResult struct {
	TenantID       string    `json:"tenant_id"`
	SegmentID      uuid.UUID `json:"segment_id"`
	EventsChained  int       `json:"events_chained"`
	HeadHash       string    `json:"head_hash"`
	LastSequenceID int64     `json:"last_sequence_id"`
}

type CreateCheckpointCommand struct {
	TenantID   string    `json:"tenant_id"`
	SegmentID  uuid.UUID `json:"segment_id"`
	KeyID      string    `json:"key_id"`
	SigningKey []byte    `json:"-"`
}

type VerifyChainQuery struct {
	TenantID   string    `json:"tenant_id"`
	SegmentID  uuid.UUID `json:"segment_id"`
	KeyID      string    `json:"key_id"`
	SecretKey  []byte    `json:"-"`
	VerifiedBy string    `json:"verified_by"`
}

type VerifyChainResult struct {
	TenantID          string             `json:"tenant_id"`
	SegmentID         uuid.UUID          `json:"segment_id"`
	Status            VerificationStatus `json:"status"`
	TotalVerified     int                `json:"total_verified"`
	HeadHash          string             `json:"head_hash"`
	FailureSequenceID *int64             `json:"failure_sequence_id,omitempty"`
	FailureReason     string             `json:"failure_reason,omitempty"`
}

type ListAuditEventsQuery struct {
	TenantID string `json:"tenant_id"`
	ActorID  string `json:"actor_id,omitempty"`
	Action   string `json:"action,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	MaskPII  bool   `json:"mask_pii,omitempty"`
}

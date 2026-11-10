package contracts

import (
	"time"

	"github.com/google/uuid"
)

type TenantStatus string

const (
	TenantStatusActive      TenantStatus = "ACTIVE"
	TenantStatusSuspended   TenantStatus = "SUSPENDED"
	TenantStatusOffboarding TenantStatus = "OFFBOARDING"
	TenantStatusOffboarded  TenantStatus = "OFFBOARDED"
)

type SuspensionReason string

const (
	SuspensionReasonBilling SuspensionReason = "BILLING"
	SuspensionReasonAbuse   SuspensionReason = "ABUSE"
)

type CommandKind string

const (
	CommandKindNewCommand          CommandKind = "NEW_COMMAND"
	CommandKindInFlightContinue    CommandKind = "IN_FLIGHT_CONTINUATION"
	CommandKindMoneyOwedToTenant   CommandKind = "MONEY_OWED_TO_TENANT"
	CommandKindScheduledJob        CommandKind = "SCHEDULED_JOB"
	CommandKindPartnerInboundEvent CommandKind = "PARTNER_INBOUND_EVENT"
)

type AdmissionAction string

const (
	AdmissionActionAdmit      AdmissionAction = "ADMIT"
	AdmissionActionReject     AdmissionAction = "REJECT"
	AdmissionActionPause      AdmissionAction = "PAUSE"
	AdmissionActionPark       AdmissionAction = "PARK"
	AdmissionActionQuarantine AdmissionAction = "QUARANTINE"
)

type QuarantineReason string

const (
	QuarantineReasonUnknownTenant     QuarantineReason = "UNKNOWN_TENANT"
	QuarantineReasonIntegrityMismatch QuarantineReason = "INTEGRITY_MISMATCH"
	QuarantineReasonOffboardedTenant  QuarantineReason = "OFFBOARDED_TENANT"
)

type QuarantineStatus string

const (
	QuarantineStatusPending   QuarantineStatus = "PENDING"
	QuarantineStatusReleased  QuarantineStatus = "RELEASED"
	QuarantineStatusDiscarded QuarantineStatus = "DISCARDED"
)

type SecurityEventType string

const (
	SecurityEventCrossTenantReference     SecurityEventType = "CROSS_TENANT_REFERENCE"
	SecurityEventTenantSelectionRejected  SecurityEventType = "TENANT_SELECTION_REJECTED"
	SecurityEventMessageIntegrityMismatch SecurityEventType = "MESSAGE_INTEGRITY_MISMATCH"
)

type TenantRecord struct {
	ID                 uuid.UUID         `json:"id"`
	PublicID           string            `json:"publicId"`
	TenantID           string            `json:"tenantId"`
	CorrelationID      uuid.UUID         `json:"correlationId"`
	Status             TenantStatus      `json:"status"`
	SuspensionReason   *SuspensionReason `json:"suspensionReason"`
	MaxConcurrentSagas int               `json:"maxConcurrentSagas"`
	StatusChangedAt    time.Time         `json:"statusChangedAt"`
	Version            int64             `json:"version"`
	CreatedAt          time.Time         `json:"createdAt"`
	UpdatedAt          time.Time         `json:"updatedAt"`
}

type SagaQuotaRecord struct {
	TenantID        string    `json:"tenantId"`
	ActiveSagaCount int64     `json:"activeSagaCount"`
	Version         int64     `json:"version"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

type SagaQuotaReservation struct {
	TenantID           string `json:"tenantId"`
	ActiveSagaCount    int64  `json:"activeSagaCount"`
	MaxConcurrentSagas int    `json:"maxConcurrentSagas"`
}

type QuarantineRecord struct {
	ID                uuid.UUID        `json:"id"`
	PublicID          string           `json:"publicId"`
	CorrelationID     uuid.UUID        `json:"correlationId"`
	SourceTopic       string           `json:"sourceTopic"`
	SourcePartition   int32            `json:"sourcePartition"`
	SourceOffset      int64            `json:"sourceOffset"`
	Reason            QuarantineReason `json:"reason"`
	Status            QuarantineStatus `json:"status"`
	EnvelopeTenantID  *string          `json:"envelopeTenantId"`
	KeyTenantID       *string          `json:"keyTenantId"`
	PayloadTenantID   *string          `json:"payloadTenantId"`
	AggregateTenantID *string          `json:"aggregateTenantId"`
	PayloadSHA256     string           `json:"payloadSha256"`
	ResolvedAt        *time.Time       `json:"resolvedAt"`
	Version           int64            `json:"version"`
	CreatedAt         time.Time        `json:"createdAt"`
	UpdatedAt         time.Time        `json:"updatedAt"`
}

type SecurityEventRecord struct {
	ID                 uuid.UUID         `json:"id"`
	PublicID           string            `json:"publicId"`
	ActorTenantID      string            `json:"actorTenantId"`
	ReferencedTenantID *string           `json:"referencedTenantId"`
	CorrelationID      uuid.UUID         `json:"correlationId"`
	EventType          SecurityEventType `json:"eventType"`
	ResourceKind       string            `json:"resourceKind"`
	ResourceID         *string           `json:"resourceId"`
	OccurredAt         time.Time         `json:"occurredAt"`
	CreatedAt          time.Time         `json:"createdAt"`
}

type CreateTenantInput struct {
	TenantID           string    `json:"tenantId"`
	MaxConcurrentSagas int       `json:"maxConcurrentSagas"`
	CorrelationID      uuid.UUID `json:"correlationId"`
}

type UpdateTenantStatusInput struct {
	TenantID         string            `json:"tenantId"`
	ExpectedVersion  int64             `json:"expectedVersion"`
	TargetStatus     TenantStatus      `json:"targetStatus"`
	SuspensionReason *SuspensionReason `json:"suspensionReason"`
}

type ResolveTenantInput struct {
	PrincipalTenantID   string    `json:"principalTenantId"`
	VerifiedMemberships []string  `json:"verifiedMemberships"`
	RequestedTenantID   *string   `json:"requestedTenantId"`
	CorrelationID       uuid.UUID `json:"correlationId"`
}

type ResolvedTenant struct {
	TenantID string `json:"tenantId"`
}

type EvaluateAdmissionInput struct {
	TenantID string      `json:"tenantId"`
	Kind     CommandKind `json:"kind"`
}

type AdmissionDecision struct {
	Action     AdmissionAction `json:"action"`
	ReasonCode string          `json:"reasonCode"`
}

type MessageTenantEnvelope struct {
	SourceTopic       string    `json:"sourceTopic"`
	SourcePartition   int32     `json:"sourcePartition"`
	SourceOffset      int64     `json:"sourceOffset"`
	EnvelopeTenantID  *string   `json:"envelopeTenantId"`
	KeyTenantID       *string   `json:"keyTenantId"`
	PayloadTenantID   *string   `json:"payloadTenantId"`
	AggregateTenantID *string   `json:"aggregateTenantId"`
	PayloadSHA256     string    `json:"payloadSha256"`
	CorrelationID     uuid.UUID `json:"correlationId"`
}

type MessageTenantDecision struct {
	Proceed          bool              `json:"proceed"`
	QuarantineReason *QuarantineReason `json:"quarantineReason"`
	TenantID         *string           `json:"tenantId"`
}

type CrossTenantReferenceInput struct {
	ActorTenantID      string    `json:"actorTenantId"`
	ReferencedTenantID string    `json:"referencedTenantId"`
	ResourceKind       string    `json:"resourceKind"`
	ResourceID         *string   `json:"resourceId"`
	CorrelationID      uuid.UUID `json:"correlationId"`
}

type UpdateSagaQuotaCounterInput struct {
	TenantID            string `json:"tenantId"`
	ExpectedVersion     int64  `json:"expectedVersion"`
	ObservedActiveSagas int64  `json:"observedActiveSagas"`
}

type ReleaseQuarantinedMessagesInput struct {
	TenantID string `json:"tenantId"`
}

type OperatorAccessGrant struct {
	OperatorID  string    `json:"operatorId"`
	TenantScope []string  `json:"tenantScope"`
	Reason      string    `json:"reason"`
	TicketID    string    `json:"ticketId"`
	IssuedAt    time.Time `json:"issuedAt"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

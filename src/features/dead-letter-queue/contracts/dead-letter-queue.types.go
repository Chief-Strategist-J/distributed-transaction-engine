package contracts

import (
	"time"

	"github.com/google/uuid"
)

type OutcomeClass string

const (
	OutcomeNotExecuted OutcomeClass = "NOT_EXECUTED"
	OutcomeUnknown     OutcomeClass = "UNKNOWN"
	OutcomeFailed      OutcomeClass = "FAILED"
)

type FailureClass string

const (
	FailurePoison              FailureClass = "POISON"
	FailureDefinitionDefect    FailureClass = "DEFINITION_DEFECT"
	FailureRetryExhausted      FailureClass = "RETRY_EXHAUSTED"
	FailureBusinessRejection   FailureClass = "BUSINESS_REJECTION"
	FailureCompensationFailure FailureClass = "COMPENSATION_FAILURE"
	FailureIntegrityViolation  FailureClass = "INTEGRITY_VIOLATION"
)

type RecordStatus string

const (
	StatusQuarantined      RecordStatus = "QUARANTINED"
	StatusTriaged          RecordStatus = "TRIAGED"
	StatusActionInProgress RecordStatus = "ACTION_IN_PROGRESS"
	StatusResolved         RecordStatus = "RESOLVED"
	StatusEscalated        RecordStatus = "ESCALATED"
)

type DispositionType string

const (
	DispositionRetriedSucceeded       DispositionType = "RETRIED_SUCCEEDED"
	DispositionResolvedAsExecuted     DispositionType = "RESOLVED_AS_EXECUTED"
	DispositionResolvedAsNotExecuted DispositionType = "RESOLVED_AS_NOT_EXECUTED"
	DispositionCorrected              DispositionType = "CORRECTED"
	DispositionSagaCompensated        DispositionType = "SAGA_COMPENSATED"
	DispositionSagaRolledForward      DispositionType = "SAGA_ROLLED_FORWARD"
	DispositionSagaCompletedManually  DispositionType = "SAGA_COMPLETED_MANUALLY"
	DispositionWrittenOff             DispositionType = "WRITTEN_OFF"
)

type DeadLetterRecord struct {
	ID                       uuid.UUID        `json:"id"`
	PublicID                 string           `json:"publicId"`
	TenantID                 string           `json:"tenantId"`
	Source                   string           `json:"source"`
	ExternalID               string           `json:"externalId"`
	CorrelationID            uuid.UUID        `json:"correlationId"`
	SagaID                   uuid.UUID        `json:"sagaId"`
	StepIndex                int              `json:"stepIndex"`
	StepType                 string           `json:"stepType"`
	AttemptID                uuid.UUID        `json:"attemptId"`
	IdempotencyKey           string           `json:"idempotencyKey"`
	OutcomeClass             OutcomeClass     `json:"outcomeClass"`
	FailureClass             FailureClass     `json:"failureClass"`
	ErrorFingerprint         string           `json:"errorFingerprint"`
	SanitizedMessage         string           `json:"sanitizedMessage"`
	PayloadRef               string           `json:"payloadRef"`
	SchemaVersion            int              `json:"schemaVersion"`
	MoneyAtStakeCents        int64            `json:"moneyAtStakeCents"`
	Currency                 string           `json:"currency"`
	BusinessDeadline         *time.Time       `json:"businessDeadline,omitempty"`
	Status                   RecordStatus     `json:"status"`
	Disposition              *DispositionType `json:"disposition,omitempty"`
	ActorID                  *string          `json:"actorId,omitempty"`
	SecondApproverID         *string          `json:"secondApproverId,omitempty"`
	DispositionReason        *string          `json:"dispositionReason,omitempty"`
	EvidenceRef              *string          `json:"evidenceRef,omitempty"`
	CorrectiveAttemptID      *uuid.UUID       `json:"correctiveAttemptId,omitempty"`
	CorrectiveIdempotencyKey *string          `json:"correctiveIdempotencyKey,omitempty"`
	Version                  int64            `json:"version"`
	CreatedAt                time.Time        `json:"createdAt"`
	UpdatedAt                time.Time        `json:"updatedAt"`
}

type SystemicIncident struct {
	ID             uuid.UUID  `json:"id"`
	PublicID       string     `json:"publicId"`
	TenantID       string     `json:"tenantId"`
	Source         string     `json:"source"`
	ExternalID     string     `json:"externalId"`
	CorrelationID  uuid.UUID  `json:"correlationId"`
	Fingerprint    string     `json:"fingerprint"`
	StepType       string     `json:"stepType"`
	IncidentStatus string     `json:"incidentStatus"`
	AffectedCount  int        `json:"affectedCount"`
	PausedAt       time.Time  `json:"pausedAt"`
	UnpausedAt     *time.Time `json:"unpausedAt,omitempty"`
	OwnerTeam      string     `json:"ownerTeam"`
	ReviewDeadline time.Time  `json:"reviewDeadline"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
}

type StepTypePause struct {
	TenantID       string    `json:"tenantId"`
	StepType       string    `json:"stepType"`
	IncidentID     uuid.UUID `json:"incidentId"`
	PausedAt       time.Time `json:"pausedAt"`
	ReviewDeadline time.Time `json:"reviewDeadline"`
	OwnerTeam      string    `json:"ownerTeam"`
}

type AdmitDLQInput struct {
	TenantID          string       `json:"tenantId"`
	CorrelationID     uuid.UUID    `json:"correlationId"`
	SagaID            uuid.UUID    `json:"sagaId"`
	StepIndex         int          `json:"stepIndex"`
	StepType          string       `json:"stepType"`
	AttemptID         uuid.UUID    `json:"attemptId"`
	IdempotencyKey    string       `json:"idempotencyKey"`
	OutcomeClass      OutcomeClass `json:"outcomeClass"`
	FailureClass      FailureClass `json:"failureClass"`
	ErrorFingerprint  string       `json:"errorFingerprint"`
	RawMessage        string       `json:"rawMessage"`
	PayloadRef        string       `json:"payloadRef"`
	SchemaVersion     int          `json:"schemaVersion"`
	MoneyAtStakeCents int64        `json:"moneyAtStakeCents"`
	Currency          string       `json:"currency"`
	BusinessDeadline  *time.Time   `json:"businessDeadline,omitempty"`
}

type TriageInput struct {
	TenantID string    `json:"tenantId"`
	RecordID uuid.UUID `json:"recordId"`
	ActorID  string    `json:"actorId"`
}

type RetryActionInput struct {
	TenantID             string    `json:"tenantId"`
	RecordID             uuid.UUID `json:"recordId"`
	ActorID              string    `json:"actorId"`
	OverrideIdempotency  bool      `json:"overrideIdempotency,omitempty"`
	OverrideConfirmation string    `json:"overrideConfirmation,omitempty"`
	OverrideReason       string    `json:"overrideReason,omitempty"`
}

type ResolveEvidenceInput struct {
	TenantID    string    `json:"tenantId"`
	RecordID    uuid.UUID `json:"recordId"`
	ActorID     string    `json:"actorId"`
	Executed    bool      `json:"executed"`
	EvidenceRef string    `json:"evidenceRef"`
	Reason      string    `json:"reason"`
}

type CorrectiveActionInput struct {
	TenantID           string    `json:"tenantId"`
	RecordID           uuid.UUID `json:"recordId"`
	ActorID            string    `json:"actorId"`
	SecondApproverID   string    `json:"secondApproverId"`
	NewAttemptID       uuid.UUID `json:"newAttemptId"`
	NewIdempotencyKey  string    `json:"newIdempotencyKey"`
	Reason             string    `json:"reason"`
}

type WriteOffInput struct {
	TenantID         string    `json:"tenantId"`
	RecordID         uuid.UUID `json:"recordId"`
	ActorID          string    `json:"actorId"`
	SecondApproverID string    `json:"secondApproverId"`
	Reason           string    `json:"reason"`
}

type ResolveSagaActionInput struct {
	TenantID    string          `json:"tenantId"`
	RecordID    uuid.UUID       `json:"recordId"`
	ActorID     string          `json:"actorId"`
	Disposition DispositionType `json:"disposition"`
	Reason      string          `json:"reason"`
}

type ReplayEvaluation struct {
	Allowed                       bool   `json:"allowed"`
	Reason                        string `json:"reason"`
	RequiresAmbiguityResolution   bool   `json:"requiresAmbiguityResolution"`
}


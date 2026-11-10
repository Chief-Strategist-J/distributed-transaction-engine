package contracts

import (
	"time"

	"github.com/google/uuid"
)

type ZeroRowCause string

const (
	CauseVersionConflict   ZeroRowCause = "VERSION_CONFLICT"
	CauseNotOwner          ZeroRowCause = "NOT_OWNER"
	CauseStateConflict     ZeroRowCause = "STATE_CONFLICT"
	CauseDataError         ZeroRowCause = "DATA_ERROR"
	CauseSecurityViolation ZeroRowCause = "SECURITY_INTEGRITY_VIOLATION"
)

type InputType string

const (
	InputTypeReply           InputType = "REPLY"
	InputTypeTimer           InputType = "TIMER"
	InputTypeOperatorCommand InputType = "OPERATOR_COMMAND"
)

type PendingInputStatus string

const (
	PendingStatusPending     PendingInputStatus = "PENDING"
	PendingStatusReprocessed PendingInputStatus = "REPROCESSED"
	PendingStatusQuarantined PendingInputStatus = "QUARANTINED"
)

type ConcurrencyStrategy string

const (
	StrategyOCC        ConcurrencyStrategy = "OCC_STANDARD"
	StrategySerialized ConcurrencyStrategy = "SERIALIZED_PARTITION"
)

type SagaRootState struct {
	ID           uuid.UUID `json:"id"`
	PublicID     string    `json:"publicId"`
	TenantID     string    `json:"tenantId"`
	CurrentState string    `json:"currentState"`
	Version      int64     `json:"version"`
	OwnerEpoch   int64     `json:"ownerEpoch"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type CASResult struct {
	Succeeded      bool         `json:"succeeded"`
	Cause          ZeroRowCause `json:"cause,omitempty"`
	CurrentVersion int64        `json:"currentVersion"`
	CurrentEpoch   int64        `json:"currentEpoch"`
	CurrentState   string       `json:"currentState"`
	ErrorMessage   string       `json:"errorMessage,omitempty"`
}

type ChildStepMutation struct {
	StepID        string `json:"stepId"`
	Status        string `json:"status"`
	OutputPayload string `json:"outputPayload"`
}

type OutboxEventPayload struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

type CASMutationInput struct {
	TenantID           string               `json:"tenantId"`
	SagaID             uuid.UUID            `json:"sagaId"`
	ExpectedVersion    int64                `json:"expectedVersion"`
	ExpectedEpoch      int64                `json:"expectedEpoch"`
	TargetState        string               `json:"targetState"`
	WorkerID           string               `json:"workerId"`
	InputID            string               `json:"inputId"`
	InputType          InputType            `json:"inputType"`
	InputPayload       string               `json:"inputPayload"`
	ChildStepMutations []ChildStepMutation  `json:"childStepMutations,omitempty"`
	OutboxEvents       []OutboxEventPayload `json:"outboxEvents,omitempty"`
}

type OCCPendingInput struct {
	ID                 uuid.UUID          `json:"id"`
	PublicID           string             `json:"publicId"`
	TenantID           string             `json:"tenantId"`
	Source             string             `json:"source"`
	ExternalID         string             `json:"externalId"`
	CorrelationID      uuid.UUID          `json:"correlationId"`
	SagaID             uuid.UUID          `json:"sagaId"`
	InputType          InputType          `json:"inputType"`
	InputPayload       string             `json:"inputPayload"`
	AttemptCount       int                `json:"attemptCount"`
	LastConflictReason string             `json:"lastConflictReason"`
	Status             PendingInputStatus `json:"status"`
	Version            int64              `json:"version"`
	CreatedAt          time.Time          `json:"createdAt"`
	UpdatedAt          time.Time          `json:"updatedAt"`
}

type ContentionJournalEntry struct {
	ID               uuid.UUID    `json:"id"`
	PublicID         string       `json:"publicId"`
	TenantID         string       `json:"tenantId"`
	SagaID           uuid.UUID    `json:"sagaId"`
	AttemptedVersion int64        `json:"attemptedVersion"`
	CurrentVersion   int64        `json:"currentVersion"`
	AttemptedEpoch   int64        `json:"attemptedEpoch"`
	CurrentEpoch     int64        `json:"currentEpoch"`
	Diagnosis        ZeroRowCause `json:"diagnosis"`
	WorkerID         string       `json:"workerId"`
	CreatedAt        time.Time    `json:"createdAt"`
}

type ClientEditInput struct {
	TenantID        string    `json:"tenantId"`
	SagaID          uuid.UUID `json:"sagaId"`
	ExpectedVersion int64     `json:"expectedVersion"`
	Parameters      string    `json:"parameters"`
	ActorID         string    `json:"actorId"`
}


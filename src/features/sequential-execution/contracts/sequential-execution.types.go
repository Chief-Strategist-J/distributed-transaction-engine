package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type ExecutionPhase string

const (
	PhaseForward      ExecutionPhase = "FORWARD"
	PhaseCompensating ExecutionPhase = "COMPENSATING"
	PhaseRollForward  ExecutionPhase = "ROLL_FORWARD"
)

type StepNature string

const (
	NatureCompensable StepNature = "COMPENSABLE"
	NaturePivot       StepNature = "PIVOT"
	NatureRetriable   StepNature = "RETRIABLE"
)

type SequentialStepOutcome string

const (
	OutcomeSucceeded       SequentialStepOutcome = "SUCCEEDED"
	OutcomeFailedRetryable SequentialStepOutcome = "FAILED_RETRYABLE"
	OutcomeFailedPermanent SequentialStepOutcome = "FAILED_PERMANENT"
	OutcomeUnknown         SequentialStepOutcome = "UNKNOWN"
)

type StartSequentialSagaInput struct {
	ID                   string                 `json:"id"`
	PublicID             string                 `json:"publicId"`
	TenantID             string                 `json:"tenantId"`
	WorkflowDefinitionID string                 `json:"workflowDefinitionId"`
	Source               string                 `json:"source"`
	ExternalID           string                 `json:"externalId"`
	CorrelationID        string                 `json:"correlationId"`
	Payload              map[string]interface{} `json:"payload"`
}

type ResumeSequentialSagaInput struct {
	TenantID        string `json:"tenantId"`
	ExpectedVersion int64  `json:"expectedVersion"`
	OwnerEpoch      int64  `json:"ownerEpoch"`
}

type SequentialSagaState struct {
	ID                   string                 `json:"id"`
	PublicID             string                 `json:"publicId"`
	TenantID             string                 `json:"tenantId"`
	WorkflowDefinitionID string                 `json:"workflowDefinitionId"`
	CurrentState         string                 `json:"currentState"`
	CurrentStepIndex     int                    `json:"currentStepIndex"`
	TotalSteps           int                    `json:"totalSteps"`
	Phase                ExecutionPhase         `json:"phase"`
	PivotReached         bool                   `json:"pivotReached"`
	Version              int64                  `json:"version"`
	OwnerEpoch           int64                  `json:"ownerEpoch"`
	FinalOutput          map[string]interface{} `json:"finalOutput"`
	CreatedAt            time.Time              `json:"createdAt"`
	UpdatedAt            time.Time              `json:"updatedAt"`
}

type StepContextEntry struct {
	ID                string                 `json:"id"`
	PublicID          string                 `json:"publicId"`
	TenantID          string                 `json:"tenantId"`
	SagaID            string                 `json:"sagaId"`
	ProducerStepIndex int                    `json:"producerStepIndex"`
	EntryName         string                 `json:"entryName"`
	EntryValue        map[string]interface{} `json:"entryValue"`
	CorrelationID     string                 `json:"correlationId"`
}

func ComputePayloadHash(payload map[string]interface{}) (string, error) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(bytes)
	return hex.EncodeToString(hash[:]), nil
}

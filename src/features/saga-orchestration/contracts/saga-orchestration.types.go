package sagaorchestration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type SagaState string

const (
	SagaStatePending           SagaState = "PENDING"
	SagaStateCreated           SagaState = "CREATED"
	SagaStateRunning           SagaState = "RUNNING"
	SagaStateRollingForward    SagaState = "ROLLING_FORWARD"
	SagaStateStuckForward      SagaState = "STUCK_FORWARD"
	SagaStateCompensating      SagaState = "COMPENSATING"
	SagaStateCompensationStuck SagaState = "COMPENSATION_STUCK"
	SagaStateCompleted         SagaState = "COMPLETED"
	SagaStateFailed            SagaState = "FAILED"
	SagaStateFailedCompensated SagaState = "FAILED_COMPENSATED"
	SagaStateCancelled         SagaState = "CANCELLED"
)

type StepExecutionState string

const (
	StepStateNotStarted   StepExecutionState = "NOT_STARTED"
	StepStatePending      StepExecutionState = "PENDING"
	StepStateDispatched   StepExecutionState = "DISPATCHED"
	StepStateRetryWait    StepExecutionState = "RETRY_WAIT"
	StepStateSucceeded    StepExecutionState = "SUCCEEDED"
	StepStateFailed       StepExecutionState = "FAILED"
	StepStateUnknown      StepExecutionState = "UNKNOWN"
	StepStateCompensating StepExecutionState = "COMPENSATING"
	StepStateCompensated  StepExecutionState = "COMPENSATED"
	StepStateSkipped      StepExecutionState = "SKIPPED"
)

type SagaStepExecution struct {
	ID             string                 `json:"id"`
	PublicID       string                 `json:"publicId"`
	TenantID       string                 `json:"tenantId"`
	SagaID         string                 `json:"sagaId"`
	StepID         string                 `json:"stepId"`
	ExecutionState StepExecutionState     `json:"executionState"`
	AttemptCount   int                    `json:"attemptCount"`
	StepInput      map[string]interface{} `json:"stepInput"`
	StepOutput     map[string]interface{} `json:"stepOutput"`
	ErrorMessage   string                 `json:"errorMessage,omitempty"`
	IdempotencyKey string                 `json:"idempotencyKey"`
	NextAttemptAt  string                 `json:"nextAttemptAt,omitempty"`
	StartedAt      string                 `json:"startedAt,omitempty"`
	CompletedAt    string                 `json:"completedAt,omitempty"`
	CreatedAt      string                 `json:"createdAt"`
	UpdatedAt      string                 `json:"updatedAt"`
}

type SagaInstance struct {
	ID                   string                 `json:"id"`
	PublicID             string                 `json:"publicId"`
	TenantID             string                 `json:"tenantId"`
	WorkflowDefinitionID string                 `json:"workflowDefinitionId"`
	WorkflowVersion      int64                  `json:"workflowVersion"`
	SpecHash             string                 `json:"specHash"`
	Source               string                 `json:"source"`
	ExternalID           string                 `json:"externalId"`
	InputHash            string                 `json:"inputHash"`
	CorrelationID        string                 `json:"correlationId"`
	CurrentState         SagaState              `json:"currentState"`
	Version              int64                  `json:"version"`
	FencingToken         int64                  `json:"fencingToken"`
	LeaseOwner           string                 `json:"leaseOwner,omitempty"`
	LeaseUntil           string                 `json:"leaseUntil,omitempty"`
	DeadlineAt           string                 `json:"deadlineAt"`
	InitialPayload       map[string]interface{} `json:"initialPayload"`
	FinalOutput          map[string]interface{} `json:"finalOutput"`
	Steps                []SagaStepExecution    `json:"steps"`
	CreatedAt            string                 `json:"createdAt"`
	UpdatedAt            string                 `json:"updatedAt"`
}

type ExecuteSagaInput struct {
	TenantID        string                 `json:"tenantId"`
	WorkflowName    string                 `json:"workflowName"`
	WorkflowVersion int64                  `json:"workflowVersion"`
	Source          string                 `json:"source"`
	ExternalID      string                 `json:"externalId"`
	CorrelationID   string                 `json:"correlationId"`
	InitialPayload  map[string]interface{} `json:"initialPayload"`
}

type ExecuteSagaResponse struct {
	Success    bool           `json:"success"`
	StatusCode int            `json:"statusCode"`
	Data       *SagaInstance  `json:"data,omitempty"`
	Error      *SagaError     `json:"error,omitempty"`
	Meta       map[string]any `json:"meta"`
}

type SagaError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type CreateSagaInput struct {
	ID       string `json:"id"`
	TenantID string `json:"tenantId"`
}

func ComputePayloadHash(payload map[string]interface{}) string {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

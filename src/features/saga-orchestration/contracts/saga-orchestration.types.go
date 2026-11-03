package sagaorchestration

type SagaState string

const (
	SagaStatePending      SagaState = "PENDING"
	SagaStateCreated      SagaState = "CREATED"
	SagaStateRunning      SagaState = "RUNNING"
	SagaStateCompensating SagaState = "COMPENSATING"
	SagaStateCompleted    SagaState = "COMPLETED"
	SagaStateFailed       SagaState = "FAILED"
)

type StepExecutionState string

const (
	StepStatePending      StepExecutionState = "PENDING"
	StepStateDispatched   StepExecutionState = "DISPATCHED"
	StepStateSucceeded    StepExecutionState = "SUCCEEDED"
	StepStateFailed       StepExecutionState = "FAILED"
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
	CorrelationID        string                 `json:"correlationId"`
	CurrentState         SagaState              `json:"currentState"`
	Version              int64                  `json:"version"`
	FencingToken         int64                  `json:"fencingToken"`
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
	Success    bool          `json:"success"`
	StatusCode int           `json:"statusCode"`
	Data       *SagaInstance `json:"data,omitempty"`
	Error      *SagaError    `json:"error,omitempty"`
	Meta       map[string]any `json:"meta"`
}

type SagaError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

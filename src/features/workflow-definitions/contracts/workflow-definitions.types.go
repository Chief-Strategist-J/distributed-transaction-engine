package contracts

type StepDefinition struct {
	StepID               string   `json:"stepId"`
	StepName             string   `json:"stepName"`
	IsPivot              bool     `json:"isPivot"`
	ActionType           string   `json:"actionType"`
	ActionEndpoint       string   `json:"actionEndpoint"`
	CompensationAction   string   `json:"compensationAction"`
	CompensationEndpoint string   `json:"compensationEndpoint"`
	TimeoutSeconds       int      `json:"timeoutSeconds"`
	MaxRetries           int      `json:"maxRetries"`
	Dependencies         []string `json:"dependencies"`
}

type WorkflowDefinitionInstance struct {
	ID            string           `json:"id"`
	PublicID      string           `json:"publicId"`
	TenantID      string           `json:"tenantId"`
	Name          string           `json:"name"`
	Version       int64            `json:"version"`
	Source        string           `json:"source"`
	ExternalID    string           `json:"externalId"`
	CorrelationID string           `json:"correlationId"`
	SpecHash      string           `json:"specHash"`
	Status        string           `json:"status"`
	TotalSteps    int              `json:"totalSteps"`
	Steps         []StepDefinition `json:"steps"`
	CreatedAt     string           `json:"createdAt"`
	UpdatedAt     string           `json:"updatedAt"`
}

type CreateWorkflowDefinitionInput struct {
	TenantID      string           `json:"tenantId"`
	Name          string           `json:"name"`
	Version       int64            `json:"version"`
	Source        string           `json:"source"`
	ExternalID    string           `json:"externalId"`
	CorrelationID string           `json:"correlationId"`
	Steps         []StepDefinition `json:"steps"`
}

type WorkflowDefinitionResponse struct {
	Success    bool                        `json:"success"`
	StatusCode int                         `json:"statusCode"`
	Data       *WorkflowDefinitionInstance `json:"data,omitempty"`
	Error      *ContractsError             `json:"error,omitempty"`
	Meta       map[string]interface{}      `json:"meta"`
}

type ContractsError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}


package sagaorchestration

type SagaState string

const (
	SagaStatePending      SagaState = "PENDING"
	SagaStateRunning      SagaState = "RUNNING"
	SagaStateCompensating SagaState = "COMPENSATING"
	SagaStateCompleted    SagaState = "COMPLETED"
	SagaStateFailed       SagaState = "FAILED"
)

type SagaInstance struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenantId"`
	State     SagaState `json:"state"`
	Version   int64     `json:"version"`
	CreatedAt string    `json:"createdAt"`
	UpdatedAt string    `json:"updatedAt"`
}

type CreateSagaInput struct {
	ID       string `json:"id"`
	TenantID string `json:"tenantId"`
}

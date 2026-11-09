package contracts

import (
	"time"
)

type CompensationPlanState string

const (
	PlanStateCompensating             CompensationPlanState = "COMPENSATING"
	PlanStateFullyCompensated         CompensationPlanState = "FULLY_COMPENSATED"
	PlanStateCompensatedWithResiduals CompensationPlanState = "COMPENSATED_WITH_RESIDUALS"
	PlanStateCompensatingNeedsAttention CompensationPlanState = "COMPENSATING_NEEDS_ATTENTION"
)

type StepCompensationStatus string

const (
	StepStatusPending             StepCompensationStatus = "PENDING"
	StepStatusDispatched          StepCompensationStatus = "DISPATCHED"
	StepStatusCompensated         StepCompensationStatus = "COMPENSATED"
	StepStatusNotCompensable      StepCompensationStatus = "NOT_COMPENSABLE"
	StepStatusPartiallyCompensated StepCompensationStatus = "PARTIALLY_COMPENSATED"
	StepStatusFailedRetryable     StepCompensationStatus = "FAILED_RETRYABLE"
	StepStatusNeedsAttention      StepCompensationStatus = "NEEDS_ATTENTION"
)

type ForwardStepExecutionRecord struct {
	StepID               string                 `json:"stepId"`
	StepName             string                 `json:"stepName"`
	IsPivot              bool                   `json:"isPivot"`
	ForwardExecutionID   string                 `json:"forwardExecutionId"`
	ForwardOutcome       string                 `json:"forwardOutcome"`
	ForwardOutput        map[string]interface{} `json:"forwardOutput"`
	CompensationAction   string                 `json:"compensationAction"`
	ExecutionOrder       int                    `json:"executionOrder"`
}

type SagaCompensationPlanInstance struct {
	ID                string                       `json:"id"`
	PublicID          string                       `json:"publicId"`
	TenantID          string                       `json:"tenantId"`
	SagaID            string                       `json:"sagaId"`
	CurrentState      CompensationPlanState        `json:"currentState"`
	TotalSteps        int                          `json:"totalSteps"`
	CompensatedSteps  int                          `json:"compensatedSteps"`
	FailedSteps       int                          `json:"failedSteps"`
	Residuals         []map[string]interface{}     `json:"residuals"`
	Steps             []SagaStepCompensationInstance `json:"steps"`
	Version           int64                        `json:"version"`
	CreatedAt         time.Time                    `json:"createdAt"`
	UpdatedAt         time.Time                    `json:"updatedAt"`
	CompletedAt       *time.Time                   `json:"completedAt,omitempty"`
}

type SagaStepCompensationInstance struct {
	ID                  string                 `json:"id"`
	PublicID            string                 `json:"publicId"`
	TenantID            string                 `json:"tenantId"`
	PlanID              string                 `json:"planId"`
	SagaID              string                 `json:"sagaId"`
	StepID              string                 `json:"stepId"`
	StepName            string                 `json:"stepName"`
	ExecutionOrder      int                    `json:"executionOrder"`
	Status              StepCompensationStatus `json:"status"`
	ForwardExecutionID  string                 `json:"forwardExecutionId"`
	CompensationAction  string                 `json:"compensationAction"`
	CompensationPayload map[string]interface{} `json:"compensationPayload"`
	ResidualPayload     map[string]interface{} `json:"residualPayload"`
	IdempotencyKey      string                 `json:"idempotencyKey"`
	AttemptCount        int                    `json:"attemptCount"`
	LastError           string                 `json:"lastError,omitempty"`
	Version             int64                  `json:"version"`
	CreatedAt           time.Time              `json:"createdAt"`
	UpdatedAt           time.Time              `json:"updatedAt"`
	CompensatedAt       *time.Time             `json:"compensatedAt,omitempty"`
}

type TriggerCompensationInput struct {
	ID           string                       `json:"id"`
	PublicID     string                       `json:"publicId"`
	TenantID     string                       `json:"tenantId"`
	SagaID       string                       `json:"sagaId"`
	Reason       string                       `json:"reason"`
	ForwardSteps []ForwardStepExecutionRecord `json:"forwardSteps"`
}

type CompleteStepCompensationInput struct {
	TenantID        string                 `json:"tenantId"`
	PlanID          string                 `json:"planId"`
	StepID          string                 `json:"stepId"`
	OutcomeStatus   StepCompensationStatus `json:"outcomeStatus"`
	ResidualPayload map[string]interface{} `json:"residualPayload,omitempty"`
	ErrorMessage    string                 `json:"errorMessage,omitempty"`
}

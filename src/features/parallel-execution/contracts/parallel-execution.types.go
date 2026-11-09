package contracts

import (
	"time"
)

type JoinPolicy string

const (
	PolicyAllRequired  JoinPolicy = "ALL_REQUIRED"
	PolicyFirstSuccess JoinPolicy = "FIRST_SUCCESS"
	PolicyNOfM         JoinPolicy = "N_OF_M"
	PolicyAllSettled   JoinPolicy = "ALL_SETTLED"
)

type BranchRole string

const (
	RoleRequired    BranchRole = "REQUIRED"
	RoleOptional    BranchRole = "OPTIONAL"
	RoleAlternative BranchRole = "ALTERNATIVE"
)

type BranchEffectType string

const (
	EffectHold        BranchEffectType = "HOLD"
	EffectCompensable BranchEffectType = "COMPENSABLE"
)

type GroupState string

const (
	GroupStateFanoutActive GroupState = "FANOUT_ACTIVE"
	GroupStateJoinSucceeded GroupState = "JOIN_SUCCEEDED"
	GroupStateJoinFailed    GroupState = "JOIN_FAILED"
	GroupStateCompensating  GroupState = "COMPENSATING"
	GroupStateCompensated   GroupState = "COMPENSATED"
)

type BranchState string

const (
	BranchStatePending        BranchState = "PENDING"
	BranchStateDispatched     BranchState = "DISPATCHED"
	BranchStateSucceeded      BranchState = "SUCCEEDED"
	BranchStateRejected       BranchState = "REJECTED"
	BranchStateFailedRetry    BranchState = "FAILED_RETRYABLE"
	BranchStateUnknown        BranchState = "UNKNOWN"
	BranchStateNotExecuted    BranchState = "NOT_EXECUTED"
	BranchStateCompensating   BranchState = "COMPENSATING"
	BranchStateCompensated    BranchState = "COMPENSATED"
)

type BranchDefinitionInput struct {
	BranchID   string                 `json:"branchId"`
	Name       string                 `json:"name"`
	Role       BranchRole             `json:"role"`
	EffectType BranchEffectType       `json:"effectType"`
	Payload    map[string]interface{} `json:"payload"`
}

type StartParallelGroupInput struct {
	ID          string                  `json:"id"`
	PublicID    string                  `json:"publicId"`
	TenantID    string                  `json:"tenantId"`
	SagaID      string                  `json:"sagaId"`
	JoinPolicy  JoinPolicy              `json:"joinPolicy"`
	QuorumCount int                     `json:"quorumCount"`
	Branches    []BranchDefinitionInput `json:"branches"`
}

type CompleteBranchInput struct {
	TenantID       string                 `json:"tenantId"`
	GroupID        string                 `json:"groupId"`
	BranchID       string                 `json:"branchId"`
	BranchPublicID string                 `json:"branchPublicId,omitempty"`
	IsSuccess      bool                   `json:"isSuccess"`
	Output         map[string]interface{} `json:"output,omitempty"`
	ErrorMessage   string                 `json:"errorMessage,omitempty"`
}

type ParallelBranchInstance struct {
	ID             string                 `json:"id"`
	PublicID       string                 `json:"publicId"`
	TenantID       string                 `json:"tenantId"`
	GroupID        string                 `json:"groupId"`
	SagaID         string                 `json:"sagaId"`
	BranchID       string                 `json:"branchId"`
	BranchName     string                 `json:"branchName"`
	Role           BranchRole             `json:"role"`
	EffectType     BranchEffectType       `json:"effectType"`
	CurrentState   BranchState            `json:"currentState"`
	IdempotencyKey string                 `json:"idempotencyKey"`
	BranchInput    map[string]interface{} `json:"branchInput"`
	BranchOutput   map[string]interface{} `json:"branchOutput"`
	ErrorMessage   string                 `json:"errorMessage,omitempty"`
	CorrelationID  string                 `json:"correlationId"`
	Version        int64                  `json:"version"`
	DispatchedAt   *time.Time             `json:"dispatchedAt,omitempty"`
	ResolvedAt     *time.Time             `json:"resolvedAt,omitempty"`
	CreatedAt      time.Time              `json:"createdAt"`
	UpdatedAt      time.Time              `json:"updatedAt"`
}

type ParallelGroupInstance struct {
	ID                string                   `json:"id"`
	PublicID          string                   `json:"publicId"`
	TenantID          string                   `json:"tenantId"`
	SagaID            string                   `json:"sagaId"`
	JoinPolicy        JoinPolicy               `json:"joinPolicy"`
	QuorumCount       int                      `json:"quorumCount"`
	CurrentState      GroupState               `json:"currentState"`
	TotalBranches     int                      `json:"totalBranches"`
	SucceededBranches int                      `json:"succeededBranches"`
	FailedBranches    int                      `json:"failedBranches"`
	Version           int64                    `json:"version"`
	CorrelationID     string                   `json:"correlationId"`
	Branches          []ParallelBranchInstance `json:"branches"`
	CreatedAt         time.Time                `json:"createdAt"`
	UpdatedAt         time.Time                `json:"updatedAt"`
}

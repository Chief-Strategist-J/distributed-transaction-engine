package contracts

import (
	"time"

	"github.com/google/uuid"
)

type InterventionActionType string

const (
	ActionForceRetry      InterventionActionType = "FORCE_RETRY"
	ActionForceCompensate InterventionActionType = "FORCE_COMPENSATE"
	ActionForceComplete   InterventionActionType = "FORCE_COMPLETE"
	ActionForceTerminate  InterventionActionType = "FORCE_TERMINATE"
)

type ManualInterventionAuditRecord struct {
	ID               uuid.UUID              `json:"id"`
	PublicID         string                 `json:"publicId"`
	TenantID         string                 `json:"tenantId"`
	Source           string                 `json:"source"`
	ExternalID       string                 `json:"externalId"`
	CorrelationID    uuid.UUID              `json:"correlationId"`
	SagaID           uuid.UUID              `json:"sagaId"`
	OperatorID       string                 `json:"operatorId"`
	OperatorRole     string                 `json:"operatorRole"`
	ActionType       InterventionActionType `json:"actionType"`
	Reason           string                 `json:"reason"`
	ApprovalTicketID string                 `json:"approvalTicketId"`
	SyntheticPayload string                 `json:"syntheticPayload"`
	OldState         string                 `json:"oldState"`
	NewState         string                 `json:"newState"`
	Version          int64                  `json:"version"`
	CreatedAt        time.Time              `json:"createdAt"`
}

type ExecuteInterventionInput struct {
	TenantID         string                 `json:"tenantId"`
	SagaID           uuid.UUID              `json:"sagaId"`
	OperatorID       string                 `json:"operatorId"`
	OperatorRole     string                 `json:"operatorRole"`
	ActionType       InterventionActionType `json:"actionType"`
	CurrentSagaState string                 `json:"currentSagaState"`
	Reason           string                 `json:"reason"`
	ApprovalTicketID string                 `json:"approvalTicketId,omitempty"`
	SyntheticPayload string                 `json:"syntheticPayload,omitempty"`
}

type InterventionExecutionResult struct {
	Success          bool                   `json:"success"`
	ActionType       InterventionActionType `json:"actionType"`
	OldState         string                 `json:"oldState"`
	NewState         string                 `json:"newState"`
	AuditRecordID    uuid.UUID              `json:"auditRecordId"`
	AuditPublicID    string                 `json:"auditPublicId"`
	ExecutionSummary string                 `json:"executionSummary"`
}

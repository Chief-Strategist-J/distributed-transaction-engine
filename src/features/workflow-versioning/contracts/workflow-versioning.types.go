package contracts

import (
	"time"

	"github.com/google/uuid"
)

type BundleStatus string

const (
	BundleStatusDraft      BundleStatus = "DRAFT"
	BundleStatusValidated  BundleStatus = "VALIDATED"
	BundleStatusActive     BundleStatus = "ACTIVE"
	BundleStatusDeprecated BundleStatus = "DEPRECATED"
	BundleStatusBlocked    BundleStatus = "BLOCKED"
	BundleStatusArchived   BundleStatus = "ARCHIVED"
)

type PromotionAction string

const (
	PromotionActionPublish   PromotionAction = "PUBLISH"
	PromotionActionPromote   PromotionAction = "PROMOTE"
	PromotionActionDeprecate PromotionAction = "DEPRECATE"
	PromotionActionBlock     PromotionAction = "BLOCK"
	PromotionActionArchive   PromotionAction = "ARCHIVE"
)

type PromotionStatus string

const (
	PromotionStatusPending  PromotionStatus = "PENDING"
	PromotionStatusApproved PromotionStatus = "APPROVED"
	PromotionStatusRejected PromotionStatus = "REJECTED"
)

type MigrationStatus string

const (
	MigrationStatusPending    MigrationStatus = "PENDING"
	MigrationStatusExecuted   MigrationStatus = "EXECUTED"
	MigrationStatusRolledBack MigrationStatus = "ROLLED_BACK"
	MigrationStatusFailed     MigrationStatus = "FAILED"
)

type WorkflowBundleRecord struct {
	ID                   uuid.UUID              `json:"id" db:"id"`
	PublicID             string                 `json:"publicId" db:"public_id"`
	TenantID             string                 `json:"tenantId" db:"tenant_id"`
	Source               string                 `json:"source" db:"source"`
	ExternalID           string                 `json:"externalId" db:"external_id"`
	CorrelationID        uuid.UUID              `json:"correlationId" db:"correlation_id"`
	WorkflowName         string                 `json:"workflowName" db:"workflow_name"`
	VersionTag           string                 `json:"versionTag" db:"version_tag"`
	ContentHash          string                 `json:"contentHash" db:"content_hash"`
	Signature            string                 `json:"signature" db:"signature"`
	Status               BundleStatus           `json:"status" db:"status"`
	BundlePayload        map[string]interface{} `json:"bundlePayload" db:"bundle_payload"`
	CapabilitiesRequired []string               `json:"capabilitiesRequired" db:"capabilities_required"`
	CreatedAt            time.Time              `json:"createdAt" db:"created_at"`
	UpdatedAt            time.Time              `json:"updatedAt" db:"updated_at"`
}

type WorkflowVersionPromotionRecord struct {
	ID            uuid.UUID       `json:"id" db:"id"`
	PublicID      string          `json:"publicId" db:"public_id"`
	TenantID      string          `json:"tenantId" db:"tenant_id"`
	Source        string          `json:"source" db:"source"`
	ExternalID    string          `json:"externalId" db:"external_id"`
	CorrelationID uuid.UUID       `json:"correlationId" db:"correlation_id"`
	BundleID      uuid.UUID       `json:"bundleId" db:"bundle_id"`
	Action        PromotionAction `json:"action" db:"action"`
	ProposerID    string          `json:"proposerId" db:"proposer_id"`
	ApproverID    string          `json:"approverId" db:"approver_id"`
	Status        PromotionStatus `json:"status" db:"status"`
	Reason        string          `json:"reason" db:"reason"`
	DecidedAt     *time.Time      `json:"decidedAt,omitempty" db:"decided_at"`
	CreatedAt     time.Time       `json:"createdAt" db:"created_at"`
	UpdatedAt     time.Time       `json:"updatedAt" db:"updated_at"`
}

type WorkflowEntityCohortRecord struct {
	ID            uuid.UUID `json:"id" db:"id"`
	PublicID      string    `json:"publicId" db:"public_id"`
	TenantID      string    `json:"tenantId" db:"tenant_id"`
	Source        string    `json:"source" db:"source"`
	ExternalID    string    `json:"externalId" db:"external_id"`
	CorrelationID uuid.UUID `json:"correlationId" db:"correlation_id"`
	WorkflowName  string    `json:"workflowName" db:"workflow_name"`
	EntityID      string    `json:"entityId" db:"entity_id"`
	CohortBucket  int       `json:"cohortBucket" db:"cohort_bucket"`
	BundleID      uuid.UUID `json:"bundleId" db:"bundle_id"`
	AssignedAt    time.Time `json:"assignedAt" db:"assigned_at"`
}

type WorkflowVersionMigrationRecord struct {
	ID                   uuid.UUID              `json:"id" db:"id"`
	PublicID             string                 `json:"publicId" db:"public_id"`
	TenantID             string                 `json:"tenantId" db:"tenant_id"`
	Source               string                 `json:"source" db:"source"`
	ExternalID           string                 `json:"externalId" db:"external_id"`
	CorrelationID        uuid.UUID              `json:"correlationId" db:"correlation_id"`
	SagaID               uuid.UUID              `json:"sagaId" db:"saga_id"`
	FromBundleID         uuid.UUID              `json:"fromBundleId" db:"from_bundle_id"`
	ToBundleID           uuid.UUID              `json:"toBundleId" db:"to_bundle_id"`
	Status               MigrationStatus        `json:"status" db:"status"`
	SafePointStepID      string                 `json:"safePointStepId" db:"safe_point_step_id"`
	StateMappingApplied  map[string]interface{} `json:"stateMappingApplied" db:"state_mapping_applied"`
	CreatedAt            time.Time              `json:"createdAt" db:"created_at"`
	UpdatedAt            time.Time              `json:"updatedAt" db:"updated_at"`
}

type RegisterBundleCommand struct {
	TenantID             string                 `json:"tenantId"`
	Source               string                 `json:"source"`
	ExternalID           string                 `json:"externalId"`
	CorrelationID        uuid.UUID              `json:"correlationId"`
	WorkflowName         string                 `json:"workflowName"`
	VersionTag           string                 `json:"versionTag"`
	Signature            string                 `json:"signature"`
	BundlePayload        map[string]interface{} `json:"bundlePayload"`
	CapabilitiesRequired []string               `json:"capabilitiesRequired"`
}

type ProposePromotionCommand struct {
	TenantID      string          `json:"tenantId"`
	Source        string          `json:"source"`
	ExternalID    string          `json:"externalId"`
	CorrelationID uuid.UUID       `json:"correlationId"`
	BundleID      uuid.UUID       `json:"bundleId"`
	Action        PromotionAction `json:"action"`
	ProposerID    string          `json:"proposerId"`
	Reason        string          `json:"reason"`
}

type DecidePromotionCommand struct {
	TenantID      string          `json:"tenantId"`
	PromotionID   uuid.UUID       `json:"promotionId"`
	ApproverID    string          `json:"approverId"`
	Approved      bool            `json:"approved"`
	Reason        string          `json:"reason"`
	CorrelationID uuid.UUID       `json:"correlationId"`
}

type UpdatePromotionDecisionInput struct {
	TenantID   string
	ID         uuid.UUID
	Status     PromotionStatus
	ApproverID string
	Reason     string
	DecidedAt  time.Time
}

type ResolveVersionQuery struct {
	TenantID       string                 `json:"tenantId"`
	WorkflowName   string                 `json:"workflowName"`
	EntityID       string                 `json:"entityId"`
	IdempotencyKey string                 `json:"idempotencyKey"`
	LiveInputs     map[string]interface{} `json:"liveInputs"`
}

type ResolveVersionResult struct {
	Bundle             *WorkflowBundleRecord  `json:"bundle"`
	IsStickyCohort     bool                   `json:"isStickyCohort"`
	LiveInputsSnapshot map[string]interface{} `json:"liveInputsSnapshot"`
}

type VerifyIntegrityQuery struct {
	TenantID     string    `json:"tenantId"`
	BundleID     uuid.UUID `json:"bundleId"`
	ExpectedHash string    `json:"expectedHash"`
}

type VerifyIntegrityResult struct {
	Valid          bool   `json:"valid"`
	ComputedHash   string `json:"computedHash"`
	IntegrityMatch bool   `json:"integrityMatch"`
}

type MigrateSagaCommand struct {
	TenantID            string                 `json:"tenantId"`
	Source              string                 `json:"source"`
	ExternalID          string                 `json:"externalId"`
	CorrelationID       uuid.UUID              `json:"correlationId"`
	SagaID              uuid.UUID              `json:"sagaId"`
	TargetVersionTag    string                 `json:"targetVersionTag"`
	SafePointStepID     string                 `json:"safePointStepId"`
	StateMappingApplied map[string]interface{} `json:"stateMappingApplied"`
	OperatorID          string                 `json:"operatorID"`
}


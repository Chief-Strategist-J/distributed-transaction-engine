package workflowversioning

import (
	"database/sql"

	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/domain/machines"
	"distributed-transaction-engine/src/features/workflow-versioning/domain/rules"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/adapters"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/ports"
	"distributed-transaction-engine/src/features/workflow-versioning/service"
)

type (
	RepositoryPort                 = ports.WorkflowVersioningRepositoryPort
	WorkflowVersioningService      = service.WorkflowVersioningService
	ServiceDeps                    = service.ServiceDeps
	WorkflowBundleRecord           = contracts.WorkflowBundleRecord
	WorkflowVersionPromotionRecord = contracts.WorkflowVersionPromotionRecord
	WorkflowEntityCohortRecord     = contracts.WorkflowEntityCohortRecord
	WorkflowVersionMigrationRecord = contracts.WorkflowVersionMigrationRecord
	RegisterBundleCommand          = contracts.RegisterBundleCommand
	ProposePromotionCommand        = contracts.ProposePromotionCommand
	DecidePromotionCommand         = contracts.DecidePromotionCommand
	UpdatePromotionDecisionInput   = contracts.UpdatePromotionDecisionInput
	ResolveVersionQuery            = contracts.ResolveVersionQuery
	ResolveVersionResult           = contracts.ResolveVersionResult
	VerifyIntegrityQuery           = contracts.VerifyIntegrityQuery
	VerifyIntegrityResult          = contracts.VerifyIntegrityResult
	MigrateSagaCommand             = contracts.MigrateSagaCommand
	BundleStatus                   = contracts.BundleStatus
	PromotionAction                = contracts.PromotionAction
	PromotionStatus                = contracts.PromotionStatus
	MigrationStatus                = contracts.MigrationStatus
	BundleMachine                  = machines.BundleMachine
	PromotionMachine               = machines.PromotionMachine
)

const (
	BundleStatusDraft      = contracts.BundleStatusDraft
	BundleStatusValidated  = contracts.BundleStatusValidated
	BundleStatusActive     = contracts.BundleStatusActive
	BundleStatusDeprecated = contracts.BundleStatusDeprecated
	BundleStatusBlocked    = contracts.BundleStatusBlocked
	BundleStatusArchived   = contracts.BundleStatusArchived

	PromotionActionPublish   = contracts.PromotionActionPublish
	PromotionActionPromote   = contracts.PromotionActionPromote
	PromotionActionDeprecate = contracts.PromotionActionDeprecate
	PromotionActionBlock     = contracts.PromotionActionBlock
	PromotionActionArchive   = contracts.PromotionActionArchive

	PromotionStatusPending  = contracts.PromotionStatusPending
	PromotionStatusApproved = contracts.PromotionStatusApproved
	PromotionStatusRejected = contracts.PromotionStatusRejected

	MigrationStatusPending    = contracts.MigrationStatusPending
	MigrationStatusExecuted   = contracts.MigrationStatusExecuted
	MigrationStatusRolledBack = contracts.MigrationStatusRolledBack
	MigrationStatusFailed     = contracts.MigrationStatusFailed
)

func NewService(deps ServiceDeps) *WorkflowVersioningService {
	return service.NewWorkflowVersioningService(deps)
}

func NewMemoryAdapter() *adapters.MemoryWorkflowVersioningRepositoryAdapter {
	return adapters.NewMemoryWorkflowVersioningRepositoryAdapter()
}

func NewPostgresAdapter(db *sql.DB) *adapters.PostgresWorkflowVersioningRepositoryAdapter {
	return adapters.NewPostgresWorkflowVersioningRepositoryAdapter(db)
}

func ComputeBundleHash(payload map[string]interface{}) (string, error) {
	return rules.ComputeBundleHash(payload)
}

func VerifyBundleIntegrity(payload map[string]interface{}, expectedHash string) error {
	return rules.VerifyBundleIntegrity(payload, expectedHash)
}

func ValidateMakerChecker(proposerID string, approverID string) error {
	return rules.ValidateMakerChecker(proposerID, approverID)
}

func MatchWorkerCapabilities(required []string, actual []string) bool {
	return rules.MatchWorkerCapabilities(required, actual)
}


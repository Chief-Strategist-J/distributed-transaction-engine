package ports

import (
	"context"

	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"github.com/google/uuid"
)

type WorkflowVersioningRepositoryPort interface {
	InsertBundle(ctx context.Context, bundle *contracts.WorkflowBundleRecord) error
	GetBundleByID(ctx context.Context, tenantID string, id uuid.UUID) (*contracts.WorkflowBundleRecord, error)
	GetBundleByVersion(ctx context.Context, tenantID string, workflowName string, versionTag string) (*contracts.WorkflowBundleRecord, error)
	GetActiveBundle(ctx context.Context, tenantID string, workflowName string) (*contracts.WorkflowBundleRecord, error)
	UpdateBundleStatus(ctx context.Context, tenantID string, id uuid.UUID, newStatus contracts.BundleStatus) error

	InsertPromotion(ctx context.Context, promo *contracts.WorkflowVersionPromotionRecord) error
	GetPromotionByID(ctx context.Context, tenantID string, id uuid.UUID) (*contracts.WorkflowVersionPromotionRecord, error)
	UpdatePromotionDecision(ctx context.Context, input contracts.UpdatePromotionDecisionInput) error

	GetEntityCohort(ctx context.Context, tenantID string, workflowName string, entityID string) (*contracts.WorkflowEntityCohortRecord, error)
	InsertEntityCohort(ctx context.Context, cohort *contracts.WorkflowEntityCohortRecord) error

	InsertMigration(ctx context.Context, migration *contracts.WorkflowVersionMigrationRecord) error
	GetMigrationByID(ctx context.Context, tenantID string, id uuid.UUID) (*contracts.WorkflowVersionMigrationRecord, error)
	UpdateMigrationStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.MigrationStatus) error
}

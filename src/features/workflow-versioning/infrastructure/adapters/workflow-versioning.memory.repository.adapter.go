package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

type MemoryWorkflowVersioningRepositoryAdapter struct {
	mu         sync.RWMutex
	bundles    map[string]*contracts.WorkflowBundleRecord
	promotions map[string]*contracts.WorkflowVersionPromotionRecord
	cohorts    map[string]*contracts.WorkflowEntityCohortRecord
	migrations map[string]*contracts.WorkflowVersionMigrationRecord
}

var _ ports.WorkflowVersioningRepositoryPort = (*MemoryWorkflowVersioningRepositoryAdapter)(nil)

func NewMemoryWorkflowVersioningRepositoryAdapter() *MemoryWorkflowVersioningRepositoryAdapter {
	return &MemoryWorkflowVersioningRepositoryAdapter{
		bundles:    make(map[string]*contracts.WorkflowBundleRecord),
		promotions: make(map[string]*contracts.WorkflowVersionPromotionRecord),
		cohorts:    make(map[string]*contracts.WorkflowEntityCohortRecord),
		migrations: make(map[string]*contracts.WorkflowVersionMigrationRecord),
	}
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) InsertBundle(ctx context.Context, bundle *contracts.WorkflowBundleRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s", bundle.TenantID, bundle.ID.String())
	nameVerKey := fmt.Sprintf("%s:%s:%s", bundle.TenantID, bundle.WorkflowName, bundle.VersionTag)
	for _, b := range a.bundles {
		if fmt.Sprintf("%s:%s:%s", b.TenantID, b.WorkflowName, b.VersionTag) == nameVerKey {
			return appErrors.NewConflictError("workflow bundle version tag already exists")
		}
	}
	a.bundles[key] = bundle
	return nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) GetBundleByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.WorkflowBundleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", tenantID, id.String())
	bundle, exists := a.bundles[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("workflow bundle not found")
	}
	return bundle, nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) GetBundleByVersion(ctx context.Context, tenantID string, workflowName string, versionTag string) (spanResult0 *contracts.WorkflowBundleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, b := range a.bundles {
		if b.TenantID == tenantID && b.WorkflowName == workflowName && b.VersionTag == versionTag {
			return b, nil
		}
	}
	return nil, appErrors.NewNotFoundError("workflow bundle version not found")
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) GetActiveBundle(ctx context.Context, tenantID string, workflowName string) (spanResult0 *contracts.WorkflowBundleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var latest *contracts.WorkflowBundleRecord
	for _, b := range a.bundles {
		if b.TenantID == tenantID && b.WorkflowName == workflowName && b.Status == contracts.BundleStatusActive {
			if latest == nil || b.CreatedAt.After(latest.CreatedAt) {
				latest = b
			}
		}
	}
	if latest == nil {
		return nil, appErrors.NewNotFoundError("active workflow bundle not found")
	}
	return latest, nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) UpdateBundleStatus(ctx context.Context, tenantID string, id uuid.UUID, newStatus contracts.BundleStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s", tenantID, id.String())
	bundle, exists := a.bundles[key]
	if !exists {
		return appErrors.NewNotFoundError("workflow bundle not found")
	}
	bundle.Status = newStatus
	bundle.UpdatedAt = time.Now()
	return nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) InsertPromotion(ctx context.Context, promo *contracts.WorkflowVersionPromotionRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s", promo.TenantID, promo.ID.String())
	a.promotions[key] = promo
	return nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) GetPromotionByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.WorkflowVersionPromotionRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", tenantID, id.String())
	promo, exists := a.promotions[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("workflow promotion not found")
	}
	return promo, nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) UpdatePromotionDecision(ctx context.Context, input contracts.UpdatePromotionDecisionInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s", input.TenantID, input.ID.String())
	promo, exists := a.promotions[key]
	if !exists {
		return appErrors.NewNotFoundError("workflow promotion not found")
	}
	promo.Status = input.Status
	promo.ApproverID = input.ApproverID
	promo.Reason = input.Reason
	promo.DecidedAt = &input.DecidedAt
	promo.UpdatedAt = time.Now()
	return nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) GetEntityCohort(ctx context.Context, tenantID string, workflowName string, entityID string) (spanResult0 *contracts.WorkflowEntityCohortRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := fmt.Sprintf("%s:%s:%s", tenantID, workflowName, entityID)
	cohort, exists := a.cohorts[key]
	if !exists {
		return nil, nil
	}
	return cohort, nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) InsertEntityCohort(ctx context.Context, cohort *contracts.WorkflowEntityCohortRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s:%s", cohort.TenantID, cohort.WorkflowName, cohort.EntityID)
	if _, exists := a.cohorts[key]; !exists {
		a.cohorts[key] = cohort
	}
	return nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) InsertMigration(ctx context.Context, migration *contracts.WorkflowVersionMigrationRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s", migration.TenantID, migration.ID.String())
	a.migrations[key] = migration
	return nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) GetMigrationByID(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.WorkflowVersionMigrationRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", tenantID, id.String())
	migration, exists := a.migrations[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("workflow migration not found")
	}
	return migration, nil
}

func (a *MemoryWorkflowVersioningRepositoryAdapter) UpdateMigrationStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.MigrationStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s:%s", tenantID, id.String())
	migration, exists := a.migrations[key]
	if !exists {
		return appErrors.NewNotFoundError("workflow migration not found")
	}
	migration.Status = status
	migration.UpdatedAt = time.Now()
	return nil
}

package ports

import (
	"context"

	contracts "distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"github.com/google/uuid"
)

type ReconciliationRepositoryPort interface {
	SaveAnomaly(ctx context.Context, anomaly contracts.ReconciliationAnomalyRecord) error
	GetAnomalyBySaga(ctx context.Context, tenantID string, sagaID uuid.UUID) (*contracts.ReconciliationAnomalyRecord, error)
	ListAnomalies(ctx context.Context, tenantID string, status *contracts.ReconciliationStatus) ([]contracts.ReconciliationAnomalyRecord, error)
	UpdateAnomalyStatus(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, nextStatus contracts.ReconciliationStatus, resolutionAction string) error
}

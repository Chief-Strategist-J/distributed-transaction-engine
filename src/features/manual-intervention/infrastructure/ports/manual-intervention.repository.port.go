package ports

import (
	"context"

	contracts "distributed-transaction-engine/src/features/manual-intervention/contracts"
	"github.com/google/uuid"
)

type ManualInterventionRepositoryPort interface {
	SaveAuditRecord(ctx context.Context, record contracts.ManualInterventionAuditRecord) error
	ListAuditRecordsBySaga(ctx context.Context, tenantID string, sagaID uuid.UUID) ([]contracts.ManualInterventionAuditRecord, error)
	ListAuditRecordsByOperator(ctx context.Context, tenantID string, operatorID string) ([]contracts.ManualInterventionAuditRecord, error)
}

package ports

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"github.com/google/uuid"
)

type AuditTrailRepositoryPort interface {
	AppendOutbox(ctx context.Context, record *contracts.OutboxEventRecord) error
	GetPendingOutbox(ctx context.Context, tenantID string, limit int) ([]contracts.OutboxEventRecord, error)
	MarkOutboxChained(ctx context.Context, tenantID string, ids []uuid.UUID, chainedAt time.Time) error

	GetTailEvent(ctx context.Context, tenantID string, segmentID uuid.UUID) (*contracts.AuditEventRecord, error)
	AppendChainedEvent(ctx context.Context, record *contracts.AuditEventRecord) error
	GetEventsBySegment(ctx context.Context, tenantID string, segmentID uuid.UUID, limit int) ([]contracts.AuditEventRecord, error)
	GetEventsByTenant(ctx context.Context, query contracts.ListAuditEventsQuery) ([]contracts.AuditEventRecord, error)
	IsAuditIDChained(ctx context.Context, tenantID string, auditID string) (bool, error)

	AppendQuarantine(ctx context.Context, record *contracts.AuditQuarantineRecord) error
	GetQuarantinedEvents(ctx context.Context, tenantID string, limit int) ([]contracts.AuditQuarantineRecord, error)

	SaveCheckpoint(ctx context.Context, record *contracts.AuditCheckpointRecord) error
	GetLatestCheckpoint(ctx context.Context, tenantID string, segmentID uuid.UUID) (*contracts.AuditCheckpointRecord, error)
	RecordVerification(ctx context.Context, record *contracts.AuditVerificationRecord) error
}

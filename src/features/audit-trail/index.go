package audittrail

import (
	"database/sql"

	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"distributed-transaction-engine/src/features/audit-trail/infrastructure/adapters"
	"distributed-transaction-engine/src/features/audit-trail/infrastructure/ports"
	"distributed-transaction-engine/src/features/audit-trail/service"
	"distributed-transaction-engine/src/infra/observability"
)

type AuditTrailService = service.AuditTrailService
type ServiceDeps = service.ServiceDeps
type AuditTrailRepositoryPort = ports.AuditTrailRepositoryPort

type CaptureOutboxEventCommand = contracts.CaptureOutboxEventCommand
type ProcessChainerBatchCommand = contracts.ProcessChainerBatchCommand
type ChainerBatchResult = contracts.ChainerBatchResult
type CreateCheckpointCommand = contracts.CreateCheckpointCommand
type VerifyChainQuery = contracts.VerifyChainQuery
type VerifyChainResult = contracts.VerifyChainResult
type ListAuditEventsQuery = contracts.ListAuditEventsQuery

type OutboxEventRecord = contracts.OutboxEventRecord
type AuditEventRecord = contracts.AuditEventRecord
type AuditQuarantineRecord = contracts.AuditQuarantineRecord
type AuditCheckpointRecord = contracts.AuditCheckpointRecord
type AuditVerificationRecord = contracts.AuditVerificationRecord

func NewService(deps ServiceDeps) *AuditTrailService {
	return service.NewAuditTrailService(deps)
}

func NewMemoryRepository() ports.AuditTrailRepositoryPort {
	return adapters.NewAuditTrailMemoryRepositoryAdapter()
}

func NewPostgresRepository(db *sql.DB) ports.AuditTrailRepositoryPort {
	return adapters.NewAuditTrailPostgresRepositoryAdapter(db)
}

func NewDefaultService(repo ports.AuditTrailRepositoryPort) *AuditTrailService {
	return service.NewAuditTrailService(service.ServiceDeps{
		Repo:   repo,
		Tracer: observability.NewTracer("audit-trail-service"),
	})
}


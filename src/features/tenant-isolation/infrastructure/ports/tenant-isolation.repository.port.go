package ports

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"github.com/google/uuid"
)

type ClockPort interface {
	Now() time.Time
}

type IDGeneratorPort interface {
	NewID() (uuid.UUID, error)
	NewPublicID(prefix string) (string, error)
}

type TenantRegistryRepositoryPort interface {
	CreateTenant(ctx context.Context, record contracts.TenantRecord) error
	FindTenantByTenantID(ctx context.Context, tenantID string) (*contracts.TenantRecord, error)
	UpdateTenantStatus(ctx context.Context, input contracts.UpdateTenantStatusInput) (contracts.TenantRecord, error)
}

type SagaQuotaRepositoryPort interface {
	FindSagaQuota(ctx context.Context, tenantID string) (*contracts.SagaQuotaRecord, error)
	CreateSagaSlotReservation(ctx context.Context, tenantID string) (*contracts.SagaQuotaReservation, error)
	DeleteSagaSlotReservation(ctx context.Context, tenantID string) (bool, error)
	UpdateSagaQuotaCounter(ctx context.Context, input contracts.UpdateSagaQuotaCounterInput) (contracts.SagaQuotaRecord, error)
}

type MessageQuarantineRepositoryPort interface {
	CreateQuarantineRecord(ctx context.Context, record contracts.QuarantineRecord) (bool, error)
	ListQuarantineRecordsByStatus(ctx context.Context, status contracts.QuarantineStatus, limit int) ([]contracts.QuarantineRecord, error)
	UpdateQuarantineRelease(ctx context.Context, tenantID string, limit int, resolvedAt time.Time) ([]contracts.QuarantineRecord, error)
}

type SecurityEventRepositoryPort interface {
	CreateSecurityEvent(ctx context.Context, record contracts.SecurityEventRecord) error
}

type TenantRegistryServicePort interface {
	CreateTenant(ctx context.Context, input contracts.CreateTenantInput) (contracts.TenantRecord, error)
	GetTenant(ctx context.Context, tenantID string) (contracts.TenantRecord, error)
	UpdateTenantStatus(ctx context.Context, input contracts.UpdateTenantStatusInput) (contracts.TenantRecord, error)
}

type SagaQuotaServicePort interface {
	CreateSagaQuotaReservation(ctx context.Context, tenantID string) (contracts.SagaQuotaReservation, error)
	DeleteSagaQuotaReservation(ctx context.Context, tenantID string) error
	GetSagaQuota(ctx context.Context, tenantID string) (contracts.SagaQuotaRecord, error)
	UpdateSagaQuotaCounter(ctx context.Context, input contracts.UpdateSagaQuotaCounterInput) (contracts.SagaQuotaRecord, error)
}

type MessageQuarantineServicePort interface {
	EvaluateMessageTenant(ctx context.Context, envelope contracts.MessageTenantEnvelope) (contracts.MessageTenantDecision, error)
	ListQuarantinedMessages(ctx context.Context, status contracts.QuarantineStatus, limit int) ([]contracts.QuarantineRecord, error)
	UpdateQuarantinedMessagesRelease(ctx context.Context, input contracts.ReleaseQuarantinedMessagesInput) ([]contracts.QuarantineRecord, error)
}

type TenantAccessServicePort interface {
	EvaluateTenantResolution(ctx context.Context, input contracts.ResolveTenantInput) (contracts.ResolvedTenant, error)
	EvaluateCommandAdmission(ctx context.Context, input contracts.EvaluateAdmissionInput) (contracts.AdmissionDecision, error)
	EvaluateCrossTenantReference(ctx context.Context, input contracts.CrossTenantReferenceInput) error
}

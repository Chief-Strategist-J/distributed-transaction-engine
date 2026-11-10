package tenantisolation

import (
	"distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/adapters"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/ports"
	"distributed-transaction-engine/src/features/tenant-isolation/service"
	"distributed-transaction-engine/src/infra/observability"
)

type (
	TenantRegistryServicePort       = ports.TenantRegistryServicePort
	SagaQuotaServicePort            = ports.SagaQuotaServicePort
	MessageQuarantineServicePort    = ports.MessageQuarantineServicePort
	TenantAccessServicePort         = ports.TenantAccessServicePort
	TenantRegistryRepositoryPort    = ports.TenantRegistryRepositoryPort
	SagaQuotaRepositoryPort         = ports.SagaQuotaRepositoryPort
	MessageQuarantineRepositoryPort = ports.MessageQuarantineRepositoryPort
	SecurityEventRepositoryPort     = ports.SecurityEventRepositoryPort
	ClockPort                       = ports.ClockPort
	IDGeneratorPort                 = ports.IDGeneratorPort
	TenantRecord                    = contracts.TenantRecord
	TenantStatus                    = contracts.TenantStatus
	SuspensionReason                = contracts.SuspensionReason
	CommandKind                     = contracts.CommandKind
	AdmissionDecision               = contracts.AdmissionDecision
	CreateTenantInput               = contracts.CreateTenantInput
	UpdateTenantStatusInput         = contracts.UpdateTenantStatusInput
	ResolveTenantInput              = contracts.ResolveTenantInput
	ResolvedTenant                  = contracts.ResolvedTenant
	EvaluateAdmissionInput          = contracts.EvaluateAdmissionInput
	MessageTenantEnvelope           = contracts.MessageTenantEnvelope
	MessageTenantDecision           = contracts.MessageTenantDecision
	CrossTenantReferenceInput       = contracts.CrossTenantReferenceInput
	SagaQuotaReservation            = contracts.SagaQuotaReservation
	SagaQuotaRecord                 = contracts.SagaQuotaRecord
	UpdateSagaQuotaCounterInput     = contracts.UpdateSagaQuotaCounterInput
	ReleaseQuarantinedMessagesInput = contracts.ReleaseQuarantinedMessagesInput
	QuarantineRecord                = contracts.QuarantineRecord
	DBTransactor                    = adapters.DBTransactor
)

type ServicesDeps struct {
	Registry   ports.TenantRegistryRepositoryPort
	Quota      ports.SagaQuotaRepositoryPort
	Quarantine ports.MessageQuarantineRepositoryPort
	Security   ports.SecurityEventRepositoryPort
	Clock      ports.ClockPort
	IDs        ports.IDGeneratorPort
	Tracer     *observability.DefaultTracer
}

type Services struct {
	Registry   *service.RegistryService
	Quota      *service.QuotaService
	Quarantine *service.QuarantineService
	Access     *service.AccessService
}

func NewServices(deps ServicesDeps) Services {
	return Services{
		Registry: service.NewRegistryService(service.RegistryServiceDeps{
			Registry: deps.Registry, Clock: deps.Clock, IDs: deps.IDs, Tracer: deps.Tracer,
		}),
		Quota: service.NewQuotaService(service.QuotaServiceDeps{
			Quota: deps.Quota, Registry: deps.Registry, Tracer: deps.Tracer,
		}),
		Quarantine: service.NewQuarantineService(service.QuarantineServiceDeps{
			Quarantine: deps.Quarantine, Registry: deps.Registry, Security: deps.Security,
			Clock: deps.Clock, IDs: deps.IDs, Tracer: deps.Tracer,
		}),
		Access: service.NewAccessService(service.AccessServiceDeps{
			Registry: deps.Registry, Security: deps.Security, Clock: deps.Clock, IDs: deps.IDs, Tracer: deps.Tracer,
		}),
	}
}

func NewMemoryAdapter() *adapters.MemoryTenantIsolationRepositoryAdapter {
	return adapters.NewMemoryTenantIsolationRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.DBTransactor) *adapters.PostgresTenantIsolationRepositoryAdapter {
	return adapters.NewPostgresTenantIsolationRepositoryAdapter(db)
}

func NewSystemRuntimeAdapter() *adapters.SystemRuntimeAdapter {
	return adapters.NewSystemRuntimeAdapter()
}

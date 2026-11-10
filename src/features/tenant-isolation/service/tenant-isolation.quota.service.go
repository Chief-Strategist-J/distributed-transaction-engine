package service

import (
	"context"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/rules"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type QuotaServiceDeps struct {
	Quota    ports.SagaQuotaRepositoryPort
	Registry ports.TenantRegistryRepositoryPort
	Tracer   *observability.DefaultTracer
}

type QuotaService struct {
	quota    ports.SagaQuotaRepositoryPort
	registry ports.TenantRegistryRepositoryPort
	tracer   *observability.DefaultTracer
}

func NewQuotaService(deps QuotaServiceDeps) *QuotaService {
	return &QuotaService{quota: deps.Quota, registry: deps.Registry, tracer: deps.Tracer}
}

func (s *QuotaService) CreateSagaQuotaReservation(ctx context.Context, tenantID string) (reservation contracts.SagaQuotaReservation, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.create_saga_quota_reservation")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateTenantID("tenantId", tenantID); err != nil {
		return contracts.SagaQuotaReservation{}, err
	}
	reserved, err := s.quota.CreateSagaSlotReservation(spanCtx, tenantID)
	if err != nil {
		return contracts.SagaQuotaReservation{}, err
	}
	if reserved == nil {
		return contracts.SagaQuotaReservation{}, s.explainReservationRefusal(spanCtx, tenantID)
	}
	return *reserved, nil
}

func (s *QuotaService) DeleteSagaQuotaReservation(ctx context.Context, tenantID string) (err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.delete_saga_quota_reservation")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateTenantID("tenantId", tenantID); err != nil {
		return err
	}
	released, err := s.quota.DeleteSagaSlotReservation(spanCtx, tenantID)
	if err != nil {
		return err
	}
	if !released {
		return appErrors.NewConflictError("tenant has no active saga reservation to release")
	}
	return nil
}

func (s *QuotaService) GetSagaQuota(ctx context.Context, tenantID string) (record contracts.SagaQuotaRecord, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.get_saga_quota")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateTenantID("tenantId", tenantID); err != nil {
		return contracts.SagaQuotaRecord{}, err
	}
	found, err := s.quota.FindSagaQuota(spanCtx, tenantID)
	if err != nil {
		return contracts.SagaQuotaRecord{}, err
	}
	if found == nil {
		return contracts.SagaQuotaRecord{}, appErrors.NewNotFoundError("tenant not found")
	}
	return *found, nil
}

func (s *QuotaService) UpdateSagaQuotaCounter(ctx context.Context, input contracts.UpdateSagaQuotaCounterInput) (record contracts.SagaQuotaRecord, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.update_saga_quota_counter")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateUpdateSagaQuotaCounterInput(input); err != nil {
		return contracts.SagaQuotaRecord{}, err
	}
	return s.quota.UpdateSagaQuotaCounter(spanCtx, input)
}

func (s *QuotaService) explainReservationRefusal(ctx context.Context, tenantID string) error {
	tenant, err := s.registry.FindTenantByTenantID(ctx, tenantID)
	if err != nil {
		return err
	}
	if tenant == nil {
		return appErrors.NewNotFoundError("tenant not found")
	}
	decision, err := rules.EvaluateAdmission(tenant.Status, tenant.SuspensionReason, contracts.CommandKindNewCommand)
	if err != nil {
		return err
	}
	if rejection := rules.RejectionError(decision); rejection != nil {
		return rejection
	}
	return appErrors.NewQuotaExceededError("concurrent saga quota exceeded")
}

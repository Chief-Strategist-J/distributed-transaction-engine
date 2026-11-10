package service

import (
	"context"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/machines"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

const (
	tenantPublicIDPrefix = "tnt_"
	initialRecordVersion = 1
)

type RegistryServiceDeps struct {
	Registry ports.TenantRegistryRepositoryPort
	Clock    ports.ClockPort
	IDs      ports.IDGeneratorPort
	Tracer   *observability.DefaultTracer
}

type RegistryService struct {
	registry ports.TenantRegistryRepositoryPort
	clock    ports.ClockPort
	ids      ports.IDGeneratorPort
	tracer   *observability.DefaultTracer
}

func NewRegistryService(deps RegistryServiceDeps) *RegistryService {
	return &RegistryService{registry: deps.Registry, clock: deps.Clock, ids: deps.IDs, tracer: deps.Tracer}
}

func (s *RegistryService) CreateTenant(ctx context.Context, input contracts.CreateTenantInput) (record contracts.TenantRecord, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.create_tenant")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateCreateTenantInput(input); err != nil {
		return contracts.TenantRecord{}, err
	}
	if record, err = s.buildTenantRecord(input); err != nil {
		return contracts.TenantRecord{}, err
	}
	if err = s.registry.CreateTenant(spanCtx, record); err != nil {
		return contracts.TenantRecord{}, err
	}
	return record, nil
}

func (s *RegistryService) GetTenant(ctx context.Context, tenantID string) (record contracts.TenantRecord, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.get_tenant")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateTenantID("tenantId", tenantID); err != nil {
		return contracts.TenantRecord{}, err
	}
	found, err := s.registry.FindTenantByTenantID(spanCtx, tenantID)
	if err != nil {
		return contracts.TenantRecord{}, err
	}
	if found == nil {
		return contracts.TenantRecord{}, appErrors.NewNotFoundError("tenant not found")
	}
	return *found, nil
}

func (s *RegistryService) UpdateTenantStatus(ctx context.Context, input contracts.UpdateTenantStatusInput) (record contracts.TenantRecord, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.update_tenant_status")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateUpdateTenantStatusInput(input); err != nil {
		return contracts.TenantRecord{}, err
	}
	current, err := s.registry.FindTenantByTenantID(spanCtx, input.TenantID)
	if err != nil {
		return contracts.TenantRecord{}, err
	}
	if current == nil {
		return contracts.TenantRecord{}, appErrors.NewNotFoundError("tenant not found")
	}
	if _, err = machines.TransitionToTenantStatus(current.Status, input.TargetStatus); err != nil {
		return contracts.TenantRecord{}, err
	}
	return s.registry.UpdateTenantStatus(spanCtx, input)
}

func (s *RegistryService) buildTenantRecord(input contracts.CreateTenantInput) (contracts.TenantRecord, error) {
	id, err := s.ids.NewID()
	if err != nil {
		return contracts.TenantRecord{}, err
	}
	publicID, err := s.ids.NewPublicID(tenantPublicIDPrefix)
	if err != nil {
		return contracts.TenantRecord{}, err
	}
	now := s.clock.Now()
	return contracts.TenantRecord{
		ID: id, PublicID: publicID, TenantID: input.TenantID, CorrelationID: input.CorrelationID,
		Status: contracts.TenantStatusActive, MaxConcurrentSagas: input.MaxConcurrentSagas,
		StatusChangedAt: now, Version: initialRecordVersion, CreatedAt: now, UpdatedAt: now,
	}, nil
}

package service

import (
	"context"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/rules"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"go.opentelemetry.io/otel/trace"
)

const tenantSelectionResourceKind = "tenant_selection"

type AccessServiceDeps struct {
	Registry ports.TenantRegistryRepositoryPort
	Security ports.SecurityEventRepositoryPort
	Clock    ports.ClockPort
	IDs      ports.IDGeneratorPort
	Tracer   *observability.DefaultTracer
}

type AccessService struct {
	registry ports.TenantRegistryRepositoryPort
	security ports.SecurityEventRepositoryPort
	clock    ports.ClockPort
	ids      ports.IDGeneratorPort
	tracer   *observability.DefaultTracer
}

func NewAccessService(deps AccessServiceDeps) *AccessService {
	return &AccessService{registry: deps.Registry, security: deps.Security, clock: deps.Clock, ids: deps.IDs, tracer: deps.Tracer}
}

func (s *AccessService) EvaluateTenantResolution(ctx context.Context, input contracts.ResolveTenantInput) (resolved contracts.ResolvedTenant, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.evaluate_tenant_resolution")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateResolveTenantInput(input); err != nil {
		return contracts.ResolvedTenant{}, err
	}
	resolved, err = rules.ResolveTenant(input)
	if err == nil {
		return resolved, nil
	}
	s.recordSecurityEvent(spanCtx, span, contracts.CrossTenantReferenceInput{
		ActorTenantID: input.PrincipalTenantID, ResourceKind: tenantSelectionResourceKind, CorrelationID: input.CorrelationID,
	}, input.RequestedTenantID, contracts.SecurityEventTenantSelectionRejected)
	return contracts.ResolvedTenant{}, err
}

func (s *AccessService) EvaluateCommandAdmission(ctx context.Context, input contracts.EvaluateAdmissionInput) (decision contracts.AdmissionDecision, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.evaluate_command_admission")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateEvaluateAdmissionInput(input); err != nil {
		return contracts.AdmissionDecision{}, err
	}
	tenant, err := s.registry.FindTenantByTenantID(spanCtx, input.TenantID)
	if err != nil {
		return contracts.AdmissionDecision{}, err
	}
	if tenant == nil {
		return contracts.AdmissionDecision{}, appErrors.NewNotFoundError("tenant not found")
	}
	if decision, err = rules.EvaluateAdmission(tenant.Status, tenant.SuspensionReason, input.Kind); err != nil {
		return contracts.AdmissionDecision{}, err
	}
	return decision, rules.RejectionError(decision)
}

func (s *AccessService) EvaluateCrossTenantReference(ctx context.Context, input contracts.CrossTenantReferenceInput) (err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.evaluate_cross_tenant_reference")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateCrossTenantReferenceInput(input); err != nil {
		return err
	}
	if !rules.IsCrossTenantReference(input.ActorTenantID, input.ReferencedTenantID) {
		return nil
	}
	s.recordSecurityEvent(spanCtx, span, input, &input.ReferencedTenantID, contracts.SecurityEventCrossTenantReference)
	return appErrors.NewNotFoundError("resource not found")
}

func (s *AccessService) recordSecurityEvent(ctx context.Context, span trace.Span, input contracts.CrossTenantReferenceInput, referenced *string, eventType contracts.SecurityEventType) {
	record, err := s.buildSecurityEvent(input, referenced, eventType)
	if err == nil {
		err = s.security.CreateSecurityEvent(ctx, record)
	}
	observability.RecordError(span, err)
}

func (s *AccessService) buildSecurityEvent(input contracts.CrossTenantReferenceInput, referenced *string, eventType contracts.SecurityEventType) (contracts.SecurityEventRecord, error) {
	id, err := s.ids.NewID()
	if err != nil {
		return contracts.SecurityEventRecord{}, err
	}
	publicID, err := s.ids.NewPublicID(securityEventPublicIDPrefix)
	if err != nil {
		return contracts.SecurityEventRecord{}, err
	}
	now := s.clock.Now()
	return contracts.SecurityEventRecord{
		ID: id, PublicID: publicID, ActorTenantID: input.ActorTenantID, ReferencedTenantID: referenced,
		CorrelationID: input.CorrelationID, EventType: eventType, ResourceKind: input.ResourceKind,
		ResourceID: input.ResourceID, OccurredAt: now, CreatedAt: now,
	}, nil
}

package service

import (
	"context"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/rules"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"go.opentelemetry.io/otel/attribute"
)

const (
	quarantinePublicIDPrefix      = "tqm_"
	securityEventPublicIDPrefix   = "tse_"
	QuarantineReleaseBatchSize    = 500
	QuarantineListMaximumLimit    = 1000
	quarantineReasonSpanAttribute = "tenant_isolation.quarantine_reason"
	securityAlertResourceKind     = "kafka_message"
)

type QuarantineServiceDeps struct {
	Quarantine ports.MessageQuarantineRepositoryPort
	Registry   ports.TenantRegistryRepositoryPort
	Security   ports.SecurityEventRepositoryPort
	Clock      ports.ClockPort
	IDs        ports.IDGeneratorPort
	Tracer     *observability.DefaultTracer
}

type QuarantineService struct {
	quarantine ports.MessageQuarantineRepositoryPort
	registry   ports.TenantRegistryRepositoryPort
	security   ports.SecurityEventRepositoryPort
	clock      ports.ClockPort
	ids        ports.IDGeneratorPort
	tracer     *observability.DefaultTracer
}

func NewQuarantineService(deps QuarantineServiceDeps) *QuarantineService {
	return &QuarantineService{
		quarantine: deps.Quarantine, registry: deps.Registry, security: deps.Security,
		clock: deps.Clock, ids: deps.IDs, tracer: deps.Tracer,
	}
}

func (s *QuarantineService) EvaluateMessageTenant(ctx context.Context, envelope contracts.MessageTenantEnvelope) (decision contracts.MessageTenantDecision, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.evaluate_message_tenant")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateMessageTenantEnvelope(envelope); err != nil {
		return contracts.MessageTenantDecision{}, err
	}
	status, err := s.findEnvelopeTenantStatus(spanCtx, envelope)
	if err != nil {
		return contracts.MessageTenantDecision{}, err
	}
	decision = rules.EvaluateMessageTenant(envelope, status)
	if decision.Proceed {
		return decision, nil
	}
	span.SetAttributes(attribute.String(quarantineReasonSpanAttribute, string(*decision.QuarantineReason)))
	return decision, s.persistQuarantine(spanCtx, envelope, *decision.QuarantineReason, status != nil)
}

func (s *QuarantineService) ListQuarantinedMessages(ctx context.Context, status contracts.QuarantineStatus, limit int) (records []contracts.QuarantineRecord, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.list_quarantined_messages")
	defer func() { observability.RecordError(span, err); span.End() }()
	if limit < 1 || limit > QuarantineListMaximumLimit {
		return nil, appErrors.NewValidationError("limit must be between 1 and 1000")
	}
	return s.quarantine.ListQuarantineRecordsByStatus(spanCtx, status, limit)
}

func (s *QuarantineService) UpdateQuarantinedMessagesRelease(ctx context.Context, input contracts.ReleaseQuarantinedMessagesInput) (released []contracts.QuarantineRecord, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "tenant_isolation.update_quarantined_messages_release")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateTenantID("tenantId", input.TenantID); err != nil {
		return nil, err
	}
	tenant, err := s.registry.FindTenantByTenantID(spanCtx, input.TenantID)
	if err != nil {
		return nil, err
	}
	if tenant == nil || tenant.Status == contracts.TenantStatusOffboarded {
		return nil, appErrors.NewConflictError("tenant is not registered, quarantined messages stay parked")
	}
	return s.quarantine.UpdateQuarantineRelease(spanCtx, input.TenantID, QuarantineReleaseBatchSize, s.clock.Now())
}

func (s *QuarantineService) findEnvelopeTenantStatus(ctx context.Context, envelope contracts.MessageTenantEnvelope) (*contracts.TenantStatus, error) {
	if envelope.EnvelopeTenantID == nil || contracts.ValidateTenantID("envelopeTenantId", *envelope.EnvelopeTenantID) != nil {
		return nil, nil
	}
	tenant, err := s.registry.FindTenantByTenantID(ctx, *envelope.EnvelopeTenantID)
	if err != nil || tenant == nil {
		return nil, err
	}
	return &tenant.Status, nil
}

func (s *QuarantineService) persistQuarantine(ctx context.Context, envelope contracts.MessageTenantEnvelope, reason contracts.QuarantineReason, tenantKnown bool) error {
	record, err := s.buildQuarantineRecord(envelope, reason)
	if err != nil {
		return err
	}
	if _, err = s.quarantine.CreateQuarantineRecord(ctx, record); err != nil {
		return err
	}
	if reason == contracts.QuarantineReasonIntegrityMismatch && tenantKnown {
		return s.persistIntegritySecurityEvent(ctx, envelope)
	}
	return nil
}

func (s *QuarantineService) buildQuarantineRecord(envelope contracts.MessageTenantEnvelope, reason contracts.QuarantineReason) (contracts.QuarantineRecord, error) {
	id, err := s.ids.NewID()
	if err != nil {
		return contracts.QuarantineRecord{}, err
	}
	publicID, err := s.ids.NewPublicID(quarantinePublicIDPrefix)
	if err != nil {
		return contracts.QuarantineRecord{}, err
	}
	now := s.clock.Now()
	return contracts.QuarantineRecord{
		ID: id, PublicID: publicID, CorrelationID: envelope.CorrelationID,
		SourceTopic: envelope.SourceTopic, SourcePartition: envelope.SourcePartition, SourceOffset: envelope.SourceOffset,
		Reason: reason, Status: contracts.QuarantineStatusPending,
		EnvelopeTenantID:  rules.BoundTenantIDForStorage(envelope.EnvelopeTenantID),
		KeyTenantID:       rules.BoundTenantIDForStorage(envelope.KeyTenantID),
		PayloadTenantID:   rules.BoundTenantIDForStorage(envelope.PayloadTenantID),
		AggregateTenantID: rules.BoundTenantIDForStorage(envelope.AggregateTenantID),
		PayloadSHA256:     envelope.PayloadSHA256, Version: initialRecordVersion, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func (s *QuarantineService) persistIntegritySecurityEvent(ctx context.Context, envelope contracts.MessageTenantEnvelope) error {
	id, err := s.ids.NewID()
	if err != nil {
		return err
	}
	publicID, err := s.ids.NewPublicID(securityEventPublicIDPrefix)
	if err != nil {
		return err
	}
	now := s.clock.Now()
	return s.security.CreateSecurityEvent(ctx, contracts.SecurityEventRecord{
		ID: id, PublicID: publicID, ActorTenantID: *envelope.EnvelopeTenantID,
		ReferencedTenantID: rules.BoundTenantIDForStorage(envelope.PayloadTenantID),
		CorrelationID:      envelope.CorrelationID, EventType: contracts.SecurityEventMessageIntegrityMismatch,
		ResourceKind: securityAlertResourceKind, OccurredAt: now, CreatedAt: now,
	})
}

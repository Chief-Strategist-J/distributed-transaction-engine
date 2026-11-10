package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/features/distributed-tracing/domain/machines"
	"distributed-transaction-engine/src/features/distributed-tracing/domain/rules"
	"distributed-transaction-engine/src/features/distributed-tracing/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo   ports.TracingRepositoryPort
	Tracer *observability.DefaultTracer
}

type DistributedTracingService struct {
	repo    ports.TracingRepositoryPort
	tracer  *observability.DefaultTracer
	machine *machines.DeepTraceSessionMachine
}

func NewDistributedTracingService(deps ServiceDeps) *DistributedTracingService {
	tracer := deps.Tracer
	if tracer == nil {
		tracer = observability.NewTracer("distributed-tracing-service")
	}
	return &DistributedTracingService{
		repo:    deps.Repo,
		tracer:  tracer,
		machine: machines.NewDeepTraceSessionMachine(),
	}
}

type StartTraceInput struct {
	TenantID          string                          `json:"tenant_id"`
	TraceparentHeader string                          `json:"traceparent_header"`
	BaggageHeader     string                          `json:"baggage_header"`
	SamplingQuery     contracts.EvaluateSamplingQuery `json:"sampling_query"`
}

func (s *DistributedTracingService) StartTrace(ctx context.Context, input StartTraceInput) (spanResult0 *contracts.TraceContext, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dt.service_start_trace")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	tc, valid := rules.ParseTraceparent(input.TraceparentHeader)
	if !valid {
		traceID := rules.GenerateTraceID()
		spanID := rules.GenerateSpanID()
		tc = &contracts.TraceContext{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: 0x01,
			IsSampled:  true,
			Baggage:    make(map[string]string),
		}
	}

	baggage := rules.ParseBaggage(input.BaggageHeader)
	tc.Baggage = baggage

	baseRate := 0.05
	rule, err := s.repo.GetSamplingRule(ctx, input.TenantID)
	if err == nil && rule != nil {
		baseRate = rule.SampleRate
	}

	tc.IsSampled = rules.EvaluateAdaptiveSampler(input.SamplingQuery, baseRate)
	if tc.IsSampled {
		tc.TraceFlags |= 0x01
	} else {
		tc.TraceFlags &= ^byte(0x01)
	}

	if _, exists := baggage["deep_mode"]; exists {
		tc.IsDeepMode = true
		tc.IsSampled = true
		tc.TraceFlags |= 0x01
	}

	return tc, nil
}

func (s *DistributedTracingService) RecordSpan(ctx context.Context, cmd contracts.RecordSpanCommand) (spanResult0 *contracts.SpanRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dt.service_record_span")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateRecordSpan(&inputCmd); err != nil {
		return nil, err
	}

	count, err := s.repo.CountSpansByTraceID(ctx, inputCmd.TenantID, inputCmd.TraceID)
	if err != nil {
		return nil, err
	}
	if count >= 1000 {
		return nil, appErrors.NewConflictError("span budget exceeded: trace already contains 1000 spans")
	}

	durationNS := inputCmd.EndTime.Sub(inputCmd.StartTime).Nanoseconds()
	if durationNS < 0 {
		durationNS = 0
	}

	sanitizedAttrs := rules.SanitizeAttributes(inputCmd.Attributes)
	sanitizedEvents := make([]contracts.SpanEvent, len(inputCmd.Events))
	for i, ev := range inputCmd.Events {
		sanitizedEvents[i] = contracts.SpanEvent{
			Name:       ev.Name,
			Timestamp:  ev.Timestamp,
			Attributes: rules.SanitizeAttributes(ev.Attributes),
		}
	}

	record := &contracts.SpanRecord{
		ID:            uuid.New(),
		PublicID:      s.generatePublicID("spn"),
		TenantID:      inputCmd.TenantID,
		Source:        "OTEL_TRACER",
		ExternalID:    inputCmd.SpanID,
		CorrelationID: uuid.New(),
		TraceID:       inputCmd.TraceID,
		SpanID:        inputCmd.SpanID,
		ParentSpanID:  inputCmd.ParentSpanID,
		SpanName:      inputCmd.SpanName,
		SpanKind:      inputCmd.SpanKind,
		StartTime:     inputCmd.StartTime.UTC(),
		EndTime:       inputCmd.EndTime.UTC(),
		DurationNS:    durationNS,
		StatusCode:    inputCmd.StatusCode,
		Attributes:    sanitizedAttrs,
		Events:        sanitizedEvents,
		Links:         inputCmd.Links,
		CreatedAt:     time.Now().UTC(),
	}

	if err := s.repo.SaveSpan(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *DistributedTracingService) RegisterAsyncLink(ctx context.Context, cmd contracts.RegisterAsyncLinkCommand) (spanResult0 *contracts.AsyncLinkRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dt.service_register_async_link")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateRegisterAsyncLink(&inputCmd); err != nil {
		return nil, err
	}

	record := &contracts.AsyncLinkRecord{
		ID:            uuid.New(),
		PublicID:      s.generatePublicID("tlr"),
		TenantID:      inputCmd.TenantID,
		Source:        "ASYNC_RELAY",
		ExternalID:    uuid.New().String(),
		CorrelationID: uuid.New(),
		SagaID:        inputCmd.SagaID,
		StepName:      inputCmd.StepName,
		Traceparent:   inputCmd.Traceparent,
		Tracestate:    inputCmd.Tracestate,
		LinkType:      inputCmd.LinkType,
		CreatedAt:     time.Now().UTC(),
	}

	if err := s.repo.SaveAsyncLink(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *DistributedTracingService) ResolveAsyncLink(ctx context.Context, query contracts.ResolveAsyncLinkQuery) (spanResult0 *contracts.SpanLink, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dt.service_resolve_async_link")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputQuery := query
	if err := contracts.ValidateResolveAsyncLink(&inputQuery); err != nil {
		return nil, err
	}

	linkRecord, err := s.repo.GetAsyncLink(ctx, inputQuery.TenantID, inputQuery.SagaID, inputQuery.StepName, inputQuery.LinkType)
	if err != nil {
		return nil, err
	}
	if linkRecord == nil {
		return nil, appErrors.NewNotFoundError("async trace context link not found")
	}

	parsed, ok := rules.ParseTraceparent(linkRecord.Traceparent)
	if !ok {
		return nil, appErrors.NewInternalError("stored traceparent is malformed")
	}

	return &contracts.SpanLink{
		TraceID: parsed.TraceID,
		SpanID:  parsed.SpanID,
		Attributes: map[string]interface{}{
			"link_type": string(linkRecord.LinkType),
			"saga_id":   linkRecord.SagaID.String(),
			"step_name": linkRecord.StepName,
		},
	}, nil
}

func (s *DistributedTracingService) ActivateDeepSession(ctx context.Context, cmd contracts.ActivateDeepSessionCommand) (spanResult0 *contracts.DeepSessionRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dt.service_activate_deep_session")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateActivateDeepSession(&inputCmd); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	record := &contracts.DeepSessionRecord{
		ID:            uuid.New(),
		PublicID:      s.generatePublicID("dts"),
		TenantID:      inputCmd.TenantID,
		Source:        "SRE_OPS",
		ExternalID:    uuid.New().String(),
		CorrelationID: uuid.New(),
		TargetType:    inputCmd.TargetType,
		TargetValue:   inputCmd.TargetValue,
		SamplingRate:  inputCmd.SamplingRate,
		MaxSpans:      inputCmd.MaxSpans,
		ExpiresAt:     now.Add(time.Duration(inputCmd.DurationSec) * time.Second),
		Status:        contracts.DeepSessionActive,
		CreatedAt:     now,
	}

	if err := s.repo.SaveDeepSession(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *DistributedTracingService) GetSpansByTraceID(ctx context.Context, tenantID string, traceID string) (spanResult0 []contracts.SpanRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dt.service_get_spans")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if tenantID == "" || traceID == "" {
		return nil, appErrors.NewValidationError("tenant_id and trace_id are required")
	}
	return s.repo.GetSpansByTraceID(ctx, tenantID, traceID)
}

func (s *DistributedTracingService) generatePublicID(prefix string) string {
	b := make([]byte, 13)
	_, swallowedErr1 := rand.Read(b)
	observability.NoteSwallowed(swallowedErr1)
	return prefix + "_" + hex.EncodeToString(b)
}

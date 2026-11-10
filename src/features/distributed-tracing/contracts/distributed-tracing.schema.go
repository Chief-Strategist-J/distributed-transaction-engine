package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"strings"
)

func ValidateRecordSpan(cmd *RecordSpanCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if len(cmd.TraceID) != 32 {
		return appErrors.NewValidationError("trace_id must be exactly 32 hex characters")
	}
	if len(cmd.SpanID) != 16 {
		return appErrors.NewValidationError("span_id must be exactly 16 hex characters")
	}
	if strings.TrimSpace(cmd.SpanName) == "" {
		return appErrors.NewValidationError("span_name is required")
	}
	if cmd.StartTime.IsZero() {
		return appErrors.NewValidationError("start_time is required")
	}
	if cmd.EndTime.IsZero() || cmd.EndTime.Before(cmd.StartTime) {
		cmd.EndTime = cmd.StartTime
	}
	return nil
}

func ValidateRegisterAsyncLink(cmd *RegisterAsyncLinkCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if cmd.SagaID == uuid.Nil {
		return appErrors.NewValidationError("saga_id is required")
	}
	if strings.TrimSpace(cmd.StepName) == "" {
		return appErrors.NewValidationError("step_name is required")
	}
	if strings.TrimSpace(cmd.Traceparent) == "" {
		return appErrors.NewValidationError("traceparent is required")
	}
	return nil
}

func ValidateResolveAsyncLink(query *ResolveAsyncLinkQuery) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if query == nil {
		return appErrors.NewValidationError("query cannot be nil")
	}
	if strings.TrimSpace(query.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if query.SagaID == uuid.Nil {
		return appErrors.NewValidationError("saga_id is required")
	}
	if strings.TrimSpace(query.StepName) == "" {
		return appErrors.NewValidationError("step_name is required")
	}
	return nil
}

func ValidateActivateDeepSession(cmd *ActivateDeepSessionCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(cmd.TargetValue) == "" {
		return appErrors.NewValidationError("target_value is required")
	}
	if cmd.SamplingRate <= 0.0 || cmd.SamplingRate > 1.0 {
		cmd.SamplingRate = 1.0
	}
	if cmd.MaxSpans <= 0 || cmd.MaxSpans > 5000 {
		cmd.MaxSpans = 1000
	}
	if cmd.DurationSec <= 0 {
		cmd.DurationSec = 300
	}
	return nil
}

func ValidateEvaluateSampling(query *EvaluateSamplingQuery) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if query == nil {
		return appErrors.NewValidationError("query cannot be nil")
	}
	if strings.TrimSpace(query.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	return nil
}

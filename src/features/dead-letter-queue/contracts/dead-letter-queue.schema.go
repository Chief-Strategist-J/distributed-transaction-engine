package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"regexp"
	"strings"
)

var (
	emailRegex = regexp.MustCompile(`(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}`)
	cardRegex  = regexp.MustCompile(`\b\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}\b`)
	tokenRegex = regexp.MustCompile(`(?i)(bearer\s+[a-z0-9\-._~+/]+=*|token\s*[:=]\s*[a-z0-9\-._~+/]+)`)
)

func SanitizeDiagnosticMessage(raw string) string {
	defer observability.TraceEnter(nil)()
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	sanitized := emailRegex.ReplaceAllString(trimmed, "[MASKED_EMAIL]")
	sanitized = cardRegex.ReplaceAllString(sanitized, "[MASKED_CARD]")
	sanitized = tokenRegex.ReplaceAllString(sanitized, "[MASKED_TOKEN]")

	if len(sanitized) > 4000 {
		return sanitized[:4000]
	}
	return sanitized
}

func ValidateAdmitInput(input AdmitDLQInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.SagaID == uuid.Nil {
		return appErrors.NewValidationError("saga_id must be a valid UUID")
	}
	if input.AttemptID == uuid.Nil {
		return appErrors.NewValidationError("attempt_id must be a valid UUID")
	}
	if input.StepIndex < 0 {
		return appErrors.NewValidationError("step_index must be greater than or equal to 0")
	}
	if strings.TrimSpace(input.StepType) == "" {
		return appErrors.NewValidationError("step_type must not be empty")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return appErrors.NewValidationError("idempotency_key must not be empty")
	}
	if strings.TrimSpace(input.ErrorFingerprint) == "" {
		return appErrors.NewValidationError("error_fingerprint must not be empty")
	}
	if strings.TrimSpace(input.PayloadRef) == "" {
		return appErrors.NewValidationError("payload_ref must not be empty")
	}
	if input.SchemaVersion < 1 {
		return appErrors.NewValidationError("schema_version must be at least 1")
	}
	if input.MoneyAtStakeCents < 0 {
		return appErrors.NewValidationError("money_at_stake_cents must not be negative")
	}
	if len(strings.TrimSpace(input.Currency)) != 3 {
		return appErrors.NewValidationError("currency must be a 3-character ISO code")
	}

	switch input.OutcomeClass {
	case OutcomeNotExecuted, OutcomeUnknown, OutcomeFailed:
	default:
		return appErrors.NewValidationError("invalid outcome_class: " + string(input.OutcomeClass))
	}

	switch input.FailureClass {
	case FailurePoison, FailureDefinitionDefect, FailureRetryExhausted, FailureBusinessRejection, FailureCompensationFailure, FailureIntegrityViolation:
	default:
		return appErrors.NewValidationError("invalid failure_class: " + string(input.FailureClass))
	}

	return nil
}

func ValidateTriageInput(input TriageInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.RecordID == uuid.Nil {
		return appErrors.NewValidationError("record_id must be a valid UUID")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return appErrors.NewValidationError("actor_id must not be empty")
	}
	return nil
}

func ValidateRetryInput(input RetryActionInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.RecordID == uuid.Nil {
		return appErrors.NewValidationError("record_id must be a valid UUID")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return appErrors.NewValidationError("actor_id must not be empty")
	}
	return nil
}

func ValidateResolveEvidenceInput(input ResolveEvidenceInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.RecordID == uuid.Nil {
		return appErrors.NewValidationError("record_id must be a valid UUID")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return appErrors.NewValidationError("actor_id must not be empty")
	}
	if strings.TrimSpace(input.EvidenceRef) == "" {
		return appErrors.NewValidationError("evidence_ref must not be empty")
	}
	if strings.TrimSpace(input.Reason) == "" {
		return appErrors.NewValidationError("reason must not be empty")
	}
	return nil
}

func ValidateCorrectiveActionInput(input CorrectiveActionInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.RecordID == uuid.Nil {
		return appErrors.NewValidationError("record_id must be a valid UUID")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return appErrors.NewValidationError("actor_id must not be empty")
	}
	if input.NewAttemptID == uuid.Nil {
		return appErrors.NewValidationError("new_attempt_id must be a valid UUID")
	}
	if strings.TrimSpace(input.NewIdempotencyKey) == "" {
		return appErrors.NewValidationError("new_idempotency_key must not be empty")
	}
	if strings.TrimSpace(input.Reason) == "" {
		return appErrors.NewValidationError("reason must not be empty")
	}
	return nil
}

func ValidateWriteOffInput(input WriteOffInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.RecordID == uuid.Nil {
		return appErrors.NewValidationError("record_id must be a valid UUID")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return appErrors.NewValidationError("actor_id must not be empty")
	}
	if strings.TrimSpace(input.Reason) == "" {
		return appErrors.NewValidationError("reason must not be empty")
	}
	return nil
}

func ValidateResolveSagaActionInput(input ResolveSagaActionInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.RecordID == uuid.Nil {
		return appErrors.NewValidationError("record_id must be a valid UUID")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return appErrors.NewValidationError("actor_id must not be empty")
	}
	if strings.TrimSpace(input.Reason) == "" {
		return appErrors.NewValidationError("reason must not be empty")
	}

	switch input.Disposition {
	case DispositionSagaCompensated, DispositionSagaRolledForward, DispositionSagaCompletedManually:
	default:
		return appErrors.NewValidationError("invalid saga disposition: " + string(input.Disposition))
	}
	return nil
}

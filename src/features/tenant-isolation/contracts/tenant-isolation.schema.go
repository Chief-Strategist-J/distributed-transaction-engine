package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"regexp"
)

const (
	MaxConcurrentSagasLowerBound = 1
	MaxConcurrentSagasUpperBound = 1000000
	MaxResourceKindLength        = 64
	MaxResourceIDLength          = 128
	MaxSourceTopicLength         = 249
	MaxVerifiedMemberships       = 256
)

var (
	tenantIDPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$`)
	payloadSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func ValidateTenantID(field string, tenantID string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if !tenantIDPattern.MatchString(tenantID) {
		return appErrors.NewValidationError(field + " must match ^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$")
	}
	return nil
}

func ValidateOptionalTenantID(field string, tenantID *string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if tenantID == nil {
		return nil
	}
	return ValidateTenantID(field, *tenantID)
}

func ValidateCreateTenantInput(input CreateTenantInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateTenantID("tenantId", input.TenantID); err != nil {
		return err
	}
	if input.MaxConcurrentSagas < MaxConcurrentSagasLowerBound || input.MaxConcurrentSagas > MaxConcurrentSagasUpperBound {
		return appErrors.NewValidationError("maxConcurrentSagas must be between 1 and 1000000")
	}
	return validateCorrelationID(input.CorrelationID)
}

func ValidateUpdateTenantStatusInput(input UpdateTenantStatusInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateTenantID("tenantId", input.TenantID); err != nil {
		return err
	}
	if input.ExpectedVersion < 1 {
		return appErrors.NewValidationError("expectedVersion must be at least 1")
	}
	return validateSuspensionReasonPairing(input.TargetStatus, input.SuspensionReason)
}

func ValidateResolveTenantInput(input ResolveTenantInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateTenantID("principalTenantId", input.PrincipalTenantID); err != nil {
		return err
	}
	if len(input.VerifiedMemberships) > MaxVerifiedMemberships {
		return appErrors.NewValidationError("verifiedMemberships exceeds the 256 entry bound")
	}
	for _, membership := range input.VerifiedMemberships {
		if err := ValidateTenantID("verifiedMemberships[]", membership); err != nil {
			return err
		}
	}
	if err := ValidateOptionalTenantID("requestedTenantId", input.RequestedTenantID); err != nil {
		return err
	}
	return validateCorrelationID(input.CorrelationID)
}

func ValidateEvaluateAdmissionInput(input EvaluateAdmissionInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateTenantID("tenantId", input.TenantID); err != nil {
		return err
	}
	switch input.Kind {
	case CommandKindNewCommand, CommandKindInFlightContinue, CommandKindMoneyOwedToTenant,
		CommandKindScheduledJob, CommandKindPartnerInboundEvent:
		return nil
	default:
		return appErrors.NewValidationError("kind is not a supported command kind")
	}
}

func ValidateMessageTenantEnvelope(input MessageTenantEnvelope) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if input.SourceTopic == "" || len(input.SourceTopic) > MaxSourceTopicLength {
		return appErrors.NewValidationError("sourceTopic must be 1..249 characters")
	}
	if input.SourcePartition < 0 || input.SourceOffset < 0 {
		return appErrors.NewValidationError("sourcePartition and sourceOffset must be non-negative")
	}
	if !payloadSHA256Pattern.MatchString(input.PayloadSHA256) {
		return appErrors.NewValidationError("payloadSha256 must be 64 lowercase hex characters")
	}
	return validateCorrelationID(input.CorrelationID)
}

func ValidateCrossTenantReferenceInput(input CrossTenantReferenceInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateTenantID("actorTenantId", input.ActorTenantID); err != nil {
		return err
	}
	if err := ValidateTenantID("referencedTenantId", input.ReferencedTenantID); err != nil {
		return err
	}
	if input.ResourceKind == "" || len(input.ResourceKind) > MaxResourceKindLength {
		return appErrors.NewValidationError("resourceKind must be 1..64 characters")
	}
	if input.ResourceID != nil && (*input.ResourceID == "" || len(*input.ResourceID) > MaxResourceIDLength) {
		return appErrors.NewValidationError("resourceId must be 1..128 characters when present")
	}
	return validateCorrelationID(input.CorrelationID)
}

func ValidateUpdateSagaQuotaCounterInput(input UpdateSagaQuotaCounterInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateTenantID("tenantId", input.TenantID); err != nil {
		return err
	}
	if input.ExpectedVersion < 1 {
		return appErrors.NewValidationError("expectedVersion must be at least 1")
	}
	if input.ObservedActiveSagas < 0 {
		return appErrors.NewValidationError("observedActiveSagas must be non-negative")
	}
	return nil
}

func validateSuspensionReasonPairing(target TenantStatus, reason *SuspensionReason) error {
	switch target {
	case TenantStatusSuspended:
		return validateSuspensionReasonPresent(reason)
	case TenantStatusActive, TenantStatusOffboarding, TenantStatusOffboarded:
		return validateSuspensionReasonAbsent(reason)
	default:
		return appErrors.NewValidationError("targetStatus is not a supported tenant status")
	}
}

func validateSuspensionReasonPresent(reason *SuspensionReason) error {
	if reason == nil {
		return appErrors.NewValidationError("suspensionReason is mandatory when targetStatus is SUSPENDED")
	}
	if *reason != SuspensionReasonBilling && *reason != SuspensionReasonAbuse {
		return appErrors.NewValidationError("suspensionReason must be BILLING or ABUSE")
	}
	return nil
}

func validateSuspensionReasonAbsent(reason *SuspensionReason) error {
	if reason != nil {
		return appErrors.NewValidationError("suspensionReason is only allowed when targetStatus is SUSPENDED")
	}
	return nil
}

func validateCorrelationID(correlationID uuid.UUID) error {
	if correlationID == uuid.Nil {
		return appErrors.NewValidationError("correlationId must be a non-nil UUID")
	}
	return nil
}

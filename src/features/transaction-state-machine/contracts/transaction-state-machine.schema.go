package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"regexp"
	"strings"
)

var (
	sagaPublicIDRegex = regexp.MustCompile(`^sag_[0-9a-zA-Z]{26}$`)
)

func ValidateInitializeStateInput(input InitializeStateInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.ID) == "" {
		return errors.NewValidationError("id is required")
	}
	if !sagaPublicIDRegex.MatchString(input.PublicID) {
		return errors.NewValidationError("publicId must match format 'sag_' + 26 alphanumeric chars")
	}
	if strings.TrimSpace(input.TenantID) == "" {
		return errors.NewValidationError("tenantId is required")
	}
	if strings.TrimSpace(input.Source) == "" {
		return errors.NewValidationError("source is required")
	}
	if strings.TrimSpace(input.ExternalID) == "" {
		return errors.NewValidationError("externalId is required")
	}
	if strings.TrimSpace(input.CorrelationID) == "" {
		return errors.NewValidationError("correlationId is required")
	}
	return nil
}

func ValidateTransitionStateInput(input TransitionStateInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return errors.NewValidationError("tenantId is required")
	}
	if strings.TrimSpace(input.SagaID) == "" {
		return errors.NewValidationError("sagaId is required")
	}
	if input.Event == "" {
		return errors.NewValidationError("event is required")
	}
	if input.ExpectedVersion < 1 {
		return errors.NewValidationError("expectedVersion must be >= 1")
	}
	if input.FencingToken < 0 {
		return errors.NewValidationError("fencingToken cannot be negative")
	}
	if strings.TrimSpace(input.Reason) == "" {
		return errors.NewValidationError("reason is required")
	}
	return nil
}

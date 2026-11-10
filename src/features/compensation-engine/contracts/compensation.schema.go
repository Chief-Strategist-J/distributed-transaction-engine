package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"regexp"
)

var (
	uuidRegex   = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-7][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	cmpPubRegex = regexp.MustCompile(`^cmp_[0-9a-zA-Z]{26}$`)
)

func ValidateTriggerCompensationInput(input *TriggerCompensationInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if input == nil {
		return errors.NewValidationError("input must not be nil")
	}
	if !uuidRegex.MatchString(input.ID) {
		return errors.NewValidationError("id must be a valid UUIDv7")
	}
	if !cmpPubRegex.MatchString(input.PublicID) {
		return errors.NewValidationError("publicId must match pattern ^cmp_[0-9a-zA-Z]{26}$")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId must not be empty")
	}
	if !uuidRegex.MatchString(input.SagaID) {
		return errors.NewValidationError("sagaId must be a valid UUID")
	}
	if input.Reason == "" {
		return errors.NewValidationError("reason must not be empty")
	}

	for _, step := range input.ForwardSteps {
		if step.StepID == "" {
			return errors.NewValidationError("stepId in forwardSteps must not be empty")
		}
		if step.IsPivot && (step.ForwardOutcome == "SUCCEEDED" || step.ForwardOutcome == "UNKNOWN") {
			return errors.NewConflictError("CE-02: compensation never crosses a succeeded or unresolved pivot step")
		}
	}

	return nil
}

func ValidateCompleteStepCompensationInput(input *CompleteStepCompensationInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if input == nil {
		return errors.NewValidationError("input must not be nil")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId must not be empty")
	}
	if !uuidRegex.MatchString(input.PlanID) {
		return errors.NewValidationError("planId must be a valid UUID")
	}
	if input.StepID == "" {
		return errors.NewValidationError("stepId must not be empty")
	}
	return nil
}

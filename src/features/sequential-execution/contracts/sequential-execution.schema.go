package contracts

import (
	"regexp"

	"distributed-transaction-engine/src/shared/errors"
)

var (
	uuidRegex   = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-7][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	seqPubRegex = regexp.MustCompile(`^seq_[0-9a-zA-Z]{26}$`)
)

func ValidateStartSequentialSagaInput(input *StartSequentialSagaInput) error {
	if input == nil {
		return errors.NewValidationError("input must not be nil")
	}
	if !uuidRegex.MatchString(input.ID) {
		return errors.NewValidationError("id must be a valid UUIDv7")
	}
	if !seqPubRegex.MatchString(input.PublicID) {
		return errors.NewValidationError("publicId must match pattern ^seq_[0-9a-zA-Z]{26}$")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId must not be empty")
	}
	if !uuidRegex.MatchString(input.WorkflowDefinitionID) {
		return errors.NewValidationError("workflowDefinitionId must be a valid UUID")
	}
	if input.Source == "" {
		return errors.NewValidationError("source must not be empty")
	}
	if input.ExternalID == "" {
		return errors.NewValidationError("externalId must not be empty")
	}
	if !uuidRegex.MatchString(input.CorrelationID) {
		return errors.NewValidationError("correlationId must be a valid UUID")
	}
	if input.Payload == nil {
		return errors.NewValidationError("payload must not be nil")
	}
	return nil
}

func ValidateResumeSequentialSagaInput(input *ResumeSequentialSagaInput) error {
	if input == nil {
		return errors.NewValidationError("input must not be nil")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId must not be empty")
	}
	if input.ExpectedVersion < 1 {
		return errors.NewValidationError("expectedVersion must be >= 1")
	}
	if input.OwnerEpoch < 1 {
		return errors.NewValidationError("ownerEpoch must be >= 1")
	}
	return nil
}

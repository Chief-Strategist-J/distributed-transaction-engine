package sagaorchestration

import (
	"fmt"
	"regexp"
	"strings"

	"distributed-transaction-engine/src/shared/errors"
)

var (
	workflowNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-\.]{3,128}$`)
	sagaPublicIDRegex = regexp.MustCompile(`^sga_[0-9a-zA-Z]{26}$`)
	stepPublicIDRegex = regexp.MustCompile(`^stx_[0-9a-zA-Z]{26}$`)
)

func ValidateExecuteSagaInput(input ExecuteSagaInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return errors.NewValidationError("tenantId cannot be empty")
	}
	if !workflowNameRegex.MatchString(input.WorkflowName) {
		return errors.NewValidationError(fmt.Sprintf("invalid workflowName format: %s", input.WorkflowName))
	}
	if input.WorkflowVersion < 1 {
		return errors.NewValidationError("workflowVersion must be greater than or equal to 1")
	}
	if strings.TrimSpace(input.Source) == "" {
		return errors.NewValidationError("source cannot be empty")
	}
	if strings.TrimSpace(input.ExternalID) == "" {
		return errors.NewValidationError("externalId cannot be empty")
	}
	if strings.TrimSpace(input.CorrelationID) == "" {
		return errors.NewValidationError("correlationId cannot be empty")
	}
	return nil
}

func ValidateSagaPublicID(publicID string) error {
	if !sagaPublicIDRegex.MatchString(publicID) {
		return errors.NewValidationError(fmt.Sprintf("invalid saga publicId: %s", publicID))
	}
	return nil
}

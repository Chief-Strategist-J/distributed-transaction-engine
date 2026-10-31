package sagaorchestration

import (
	"distributed-transaction-engine/src/shared/errors"
)

func ValidateCreateSagaInput(input CreateSagaInput) error {
	if input.ID == "" {
		return errors.NewValidationError("id is required")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId is required")
	}
	return nil
}

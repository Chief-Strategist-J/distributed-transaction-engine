package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"strings"
)

func ValidateCASMutationInput(input CASMutationInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.SagaID == uuid.Nil {
		return appErrors.NewValidationError("saga_id must be a valid UUID")
	}
	if input.ExpectedVersion < 0 {
		return appErrors.NewValidationError("expected_version must not be negative")
	}
	if input.ExpectedEpoch < 0 {
		return appErrors.NewValidationError("expected_epoch must not be negative")
	}
	if strings.TrimSpace(input.TargetState) == "" {
		return appErrors.NewValidationError("target_state must not be empty")
	}
	if strings.TrimSpace(input.WorkerID) == "" {
		return appErrors.NewValidationError("worker_id must not be empty")
	}
	if strings.TrimSpace(input.InputID) == "" {
		return appErrors.NewValidationError("input_id must not be empty")
	}

	switch input.InputType {
	case InputTypeReply, InputTypeTimer, InputTypeOperatorCommand:
	default:
		return appErrors.NewValidationError("invalid input_type: " + string(input.InputType))
	}

	return nil
}

func ValidateClientEditInput(input ClientEditInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id must not be empty")
	}
	if input.SagaID == uuid.Nil {
		return appErrors.NewValidationError("saga_id must be a valid UUID")
	}
	if input.ExpectedVersion < 1 {
		return appErrors.NewValidationError("expected_version must be at least 1")
	}
	if strings.TrimSpace(input.Parameters) == "" {
		return appErrors.NewValidationError("parameters must not be empty")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return appErrors.NewValidationError("actor_id must not be empty")
	}
	return nil
}

package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"strings"
)

func ValidateEvaluateReconciliationInput(input EvaluateReconciliationInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	if input.SagaID.String() == "00000000-0000-0000-0000-000000000000" {
		return appErrors.NewValidationError("sagaId must be a valid non-nil UUID")
	}
	if strings.TrimSpace(input.InternalStatus) == "" {
		return appErrors.NewValidationError("internalStatus cannot be empty")
	}
	if strings.TrimSpace(input.ExternalStatus) == "" {
		return appErrors.NewValidationError("externalStatus cannot be empty")
	}
	if input.InternalAmountCents < 0 {
		return appErrors.NewValidationError("internalAmountCents must be non-negative")
	}
	if input.ExternalAmountCents < 0 {
		return appErrors.NewValidationError("externalAmountCents must be non-negative")
	}
	currency := strings.TrimSpace(strings.ToUpper(input.Currency))
	if len(currency) != 3 {
		return appErrors.NewValidationError("currency must be 3-character ISO code")
	}
	return nil
}

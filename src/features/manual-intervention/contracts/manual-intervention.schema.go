package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"strings"
)

func ValidateExecuteInterventionInput(input ExecuteInterventionInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	if input.SagaID.String() == "00000000-0000-0000-0000-000000000000" {
		return appErrors.NewValidationError("sagaId must be a valid non-nil UUID")
	}
	if strings.TrimSpace(input.OperatorID) == "" {
		return appErrors.NewValidationError("operatorId cannot be empty")
	}
	if strings.TrimSpace(input.OperatorRole) == "" {
		return appErrors.NewValidationError("operatorRole cannot be empty")
	}
	if strings.TrimSpace(input.Reason) == "" {
		return appErrors.NewValidationError("reason cannot be empty")
	}
	if input.ActionType == ActionForceComplete && strings.TrimSpace(input.ApprovalTicketID) == "" {
		return appErrors.NewValidationError("approvalTicketId is mandatory for FORCE_COMPLETE")
	}
	return nil
}

package rules

import (
	contracts "distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"strings"
)

const RequiredInterventionRole = "admin:saga:intervene"

type ManualInterventionRules struct{}

func NewManualInterventionRules() *ManualInterventionRules {
	return &ManualInterventionRules{}
}

func (r *ManualInterventionRules) ValidateOperatorRole(role string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return CheckOperatorRole(role)
}

func (r *ManualInterventionRules) ValidateTargetSagaState(state string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return CheckTargetSagaState(state)
}

func (r *ManualInterventionRules) DeriveNextState(action contracts.InterventionActionType) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return DetermineNextState(action)
}

func CheckOperatorRole(role string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(role) != RequiredInterventionRole {
		return appErrors.NewConflictError("Unauthorized operator role for manual intervention")
	}
	return nil
}

func CheckTargetSagaState(state string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	norm := strings.ToUpper(strings.TrimSpace(state))
	switch norm {
	case "STUCK", "FAILED", "WAITING":
		return nil
	case "RUNNING", "COMPLETED":
		return appErrors.NewConflictError("Cannot intervene on active RUNNING or COMPLETED saga")
	default:
		return appErrors.NewConflictError("Saga is not in an intervenable state")
	}
}

func DetermineNextState(action contracts.InterventionActionType) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch action {
	case contracts.ActionForceRetry:
		return "RUNNING", nil
	case contracts.ActionForceCompensate:
		return "COMPENSATING", nil
	case contracts.ActionForceComplete:
		return "COMPLETED", nil
	case contracts.ActionForceTerminate:
		return "FAILED_TERMINATED", nil
	default:
		return "", appErrors.NewValidationError("Unknown intervention action type")
	}
}

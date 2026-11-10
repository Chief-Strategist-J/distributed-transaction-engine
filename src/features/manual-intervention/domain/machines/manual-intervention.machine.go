package machines

import (
	contracts "distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type ManualInterventionMachine struct{}

func NewManualInterventionMachine() *ManualInterventionMachine {
	return &ManualInterventionMachine{}
}

func (m *ManualInterventionMachine) ValidateActionEligibility(action contracts.InterventionActionType, currentSagaState string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch action {
	case contracts.ActionForceRetry, contracts.ActionForceCompensate, contracts.ActionForceComplete, contracts.ActionForceTerminate:
		if currentSagaState == "COMPLETED" || currentSagaState == "RUNNING" {
			return appErrors.NewConflictError("Action cannot be applied to RUNNING or COMPLETED sagas")
		}
		return nil
	default:
		return appErrors.NewValidationError("Unsupported intervention action")
	}
}

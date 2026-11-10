package machines

import (
	contracts "distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type ReconciliationMachine struct{}

func NewReconciliationMachine() *ReconciliationMachine {
	return &ReconciliationMachine{}
}

func (m *ReconciliationMachine) CanTransition(from contracts.ReconciliationStatus, to contracts.ReconciliationStatus) bool {
	defer observability.TraceEnter(nil)()
	switch from {
	case contracts.StatusAnomalyDetected:
		return to == contracts.StatusRepairScheduled || to == contracts.StatusAutoResolved || to == contracts.StatusQuarantinedForAudit
	case contracts.StatusRepairScheduled:
		return to == contracts.StatusAutoResolved || to == contracts.StatusQuarantinedForAudit
	case contracts.StatusQuarantinedForAudit:
		return to == contracts.StatusAutoResolved || to == contracts.StatusManualDismissed
	default:
		return false
	}
}

func (m *ReconciliationMachine) Transition(from contracts.ReconciliationStatus, to contracts.ReconciliationStatus) (spanResult0 contracts.ReconciliationStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if !m.CanTransition(from, to) {
		return from, appErrors.NewConflictError("Illegal reconciliation transition from " + string(from) + " to " + string(to))
	}
	return to, nil
}

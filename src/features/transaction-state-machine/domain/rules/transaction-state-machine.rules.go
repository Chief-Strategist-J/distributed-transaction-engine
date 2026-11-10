package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	"distributed-transaction-engine/src/infra/observability"
)

func IsTerminal(state contracts.TransactionState) bool {
	defer observability.TraceEnter(nil)()
	switch state {
	case contracts.StateCompleted, contracts.StateCancelled, contracts.StateCompensated, contracts.StateFailed:
		return true
	default:
		return false
	}
}

func CanTransition(from contracts.TransactionState, to contracts.TransactionState) bool {
	defer observability.TraceEnter(nil)()
	if IsTerminal(from) {
		return false
	}
	switch from {
	case contracts.StateCreated:
		return to == contracts.StateRunning || to == contracts.StateCancelled
	case contracts.StateRunning:
		return to == contracts.StateRunning || to == contracts.StateCompleted || to == contracts.StateCompensating || to == contracts.StateThrottledWait || to == contracts.StateReconciling || to == contracts.StateWaiting
	case contracts.StateWaiting:
		return to == contracts.StateRunning || to == contracts.StateCompensating
	case contracts.StateThrottledWait:
		return to == contracts.StateRunning
	case contracts.StateReconciling:
		return to == contracts.StateRunning || to == contracts.StateCompensating || to == contracts.StateStuck
	case contracts.StateCompensating:
		return to == contracts.StateCompensating || to == contracts.StateCompensated || to == contracts.StateStuck
	case contracts.StateStuck:
		return to == contracts.StateCompensating || to == contracts.StateFailed
	default:
		return false
	}
}

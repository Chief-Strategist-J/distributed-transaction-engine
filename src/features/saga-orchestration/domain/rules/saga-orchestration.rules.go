package rules

import (
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

func CanTransition(from sagaContracts.SagaState, to sagaContracts.SagaState) bool {
	switch from {
	case sagaContracts.SagaStatePending:
		return to == sagaContracts.SagaStateRunning || to == sagaContracts.SagaStateFailed
	case sagaContracts.SagaStateRunning:
		return to == sagaContracts.SagaStateCompensating || to == sagaContracts.SagaStateCompleted || to == sagaContracts.SagaStateFailed
	case sagaContracts.SagaStateCompensating:
		return to == sagaContracts.SagaStateFailed
	default:
		return false
	}
}

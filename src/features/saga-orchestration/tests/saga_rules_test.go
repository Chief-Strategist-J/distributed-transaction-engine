package tests

import (
	"testing"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
)

func TestCanTransition(t *testing.T) {
	if !sagaRules.CanTransition(sagaContracts.SagaStatePending, sagaContracts.SagaStateRunning) {
		t.Errorf("expected transition to be valid")
	}
	if sagaRules.CanTransition(sagaContracts.SagaStatePending, sagaContracts.SagaStateCompleted) {
		t.Errorf("expected transition to be invalid")
	}
}

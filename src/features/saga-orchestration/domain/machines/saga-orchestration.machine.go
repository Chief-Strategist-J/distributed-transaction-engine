package machines

import (
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
	"distributed-transaction-engine/src/shared/errors"
)

type SagaStateMachine struct{}

func NewSagaStateMachine() *SagaStateMachine {
	return &SagaStateMachine{}
}

func (m *SagaStateMachine) TransitionTo(current sagaContracts.SagaState, next sagaContracts.SagaState) (sagaContracts.SagaState, error) {
	if !sagaRules.CanTransition(current, next) {
		return current, errors.NewConflictError("invalid state transition")
	}
	return next, nil
}

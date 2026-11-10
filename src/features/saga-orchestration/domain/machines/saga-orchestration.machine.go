package machines

import (
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type SagaStateMachine struct{}

func NewSagaStateMachine() *SagaStateMachine {
	return &SagaStateMachine{}
}

func (m *SagaStateMachine) TransitionTo(current sagaContracts.SagaState, next sagaContracts.SagaState) (spanResult0 sagaContracts.SagaState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if !sagaRules.CanTransition(current, next) {
		return current, errors.NewConflictError("invalid state transition")
	}
	return next, nil
}

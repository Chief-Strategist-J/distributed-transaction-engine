package machines

import (
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	rules "distributed-transaction-engine/src/features/transaction-state-machine/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type TransactionStateMachine struct{}

func NewTransactionStateMachine() *TransactionStateMachine {
	return &TransactionStateMachine{}
}

func (m *TransactionStateMachine) ComputeNextState(current contracts.TransactionState, event contracts.StateTransitionEvent) (spanResult0 contracts.TransactionState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if rules.IsTerminal(current) {
		return current, errors.NewConflictError("cannot transition from terminal state")
	}

	nextState, err := resolveNextState(current, event)
	if err != nil {
		return current, err
	}

	if !rules.CanTransition(current, nextState) {
		return current, errors.NewConflictError("transition disallowed by domain rules")
	}

	return nextState, nil
}

func resolveNextState(current contracts.TransactionState, event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch current {
	case contracts.StateCreated:
		return handleCreatedState(event)
	case contracts.StateRunning:
		return handleRunningState(event)
	case contracts.StateWaiting:
		return handleWaitingState(event)
	case contracts.StateThrottledWait:
		return handleThrottledWaitState(event)
	case contracts.StateReconciling:
		return handleReconcilingState(event)
	case contracts.StateCompensating:
		return handleCompensatingState(event)
	case contracts.StateStuck:
		return handleStuckState(event)
	default:
		return current, errors.NewConflictError("unhandled current state")
	}
}

func handleCreatedState(event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch event {
	case contracts.EventOrchestratorClaim:
		return contracts.StateRunning, nil
	case contracts.EventClientAbort:
		return contracts.StateCancelled, nil
	default:
		return contracts.StateCreated, errors.NewConflictError("invalid event for CREATED state")
	}
}

func handleRunningState(event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch event {
	case contracts.EventStepSuccess:
		return contracts.StateRunning, nil
	case contracts.EventDAGCompleted:
		return contracts.StateCompleted, nil
	case contracts.EventBusinessFailure:
		return contracts.StateCompensating, nil
	case contracts.EventRateLimited:
		return contracts.StateThrottledWait, nil
	case contracts.EventIndeterminateError:
		return contracts.StateReconciling, nil
	case contracts.EventAsyncSuspend:
		return contracts.StateWaiting, nil
	default:
		return contracts.StateRunning, errors.NewConflictError("invalid event for RUNNING state")
	}
}

func handleWaitingState(event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch event {
	case contracts.EventWebhookApproved:
		return contracts.StateRunning, nil
	case contracts.EventWebhookRejected, contracts.EventWebhookTimeout:
		return contracts.StateCompensating, nil
	default:
		return contracts.StateWaiting, errors.NewConflictError("invalid event for WAITING state")
	}
}

func handleThrottledWaitState(event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch event {
	case contracts.EventThrottleTimerFired:
		return contracts.StateRunning, nil
	default:
		return contracts.StateThrottledWait, errors.NewConflictError("invalid event for THROTTLED_WAIT state")
	}
}

func handleReconcilingState(event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch event {
	case contracts.EventProbeSucceeded, contracts.EventProbeNotFound:
		return contracts.StateRunning, nil
	case contracts.EventProbeBudgetExpired:
		return contracts.StateStuck, nil
	case contracts.EventBusinessFailure:
		return contracts.StateCompensating, nil
	default:
		return contracts.StateReconciling, errors.NewConflictError("invalid event for RECONCILING state")
	}
}

func handleCompensatingState(event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch event {
	case contracts.EventCompensationStepOK:
		return contracts.StateCompensating, nil
	case contracts.EventCompensationDone:
		return contracts.StateCompensated, nil
	case contracts.EventCompensationHalt:
		return contracts.StateStuck, nil
	default:
		return contracts.StateCompensating, errors.NewConflictError("invalid event for COMPENSATING state")
	}
}

func handleStuckState(event contracts.StateTransitionEvent) (contracts.TransactionState, error) {
	switch event {
	case contracts.EventOperatorRedrive:
		return contracts.StateCompensating, nil
	case contracts.EventOperatorWriteOff:
		return contracts.StateFailed, nil
	default:
		return contracts.StateStuck, errors.NewConflictError("invalid event for STUCK state")
	}
}

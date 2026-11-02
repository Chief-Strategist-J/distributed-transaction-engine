package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	machines "distributed-transaction-engine/src/features/transaction-state-machine/domain/machines"
)

func TestMachineTransitions(t *testing.T) {
	m := machines.NewTransactionStateMachine()

	next, err := m.ComputeNextState(contracts.StateCreated, contracts.EventOrchestratorClaim)
	if err != nil || next != contracts.StateRunning {
		t.Fatalf("expected RUNNING: %v", err)
	}

	next, err = m.ComputeNextState(contracts.StateRunning, contracts.EventRateLimited)
	if err != nil || next != contracts.StateThrottledWait {
		t.Fatalf("expected THROTTLED_WAIT: %v", err)
	}

	next, err = m.ComputeNextState(contracts.StateThrottledWait, contracts.EventThrottleTimerFired)
	if err != nil || next != contracts.StateRunning {
		t.Fatalf("expected RUNNING: %v", err)
	}

	next, err = m.ComputeNextState(contracts.StateRunning, contracts.EventIndeterminateError)
	if err != nil || next != contracts.StateReconciling {
		t.Fatalf("expected RECONCILING: %v", err)
	}

	next, err = m.ComputeNextState(contracts.StateReconciling, contracts.EventProbeBudgetExpired)
	if err != nil || next != contracts.StateStuck {
		t.Fatalf("expected STUCK: %v", err)
	}

	next, err = m.ComputeNextState(contracts.StateStuck, contracts.EventOperatorRedrive)
	if err != nil || next != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}

	next, err = m.ComputeNextState(contracts.StateCompensating, contracts.EventCompensationDone)
	if err != nil || next != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}

	_, err = m.ComputeNextState(contracts.StateCompensated, contracts.EventOrchestratorClaim)
	if err == nil {
		t.Fatalf("expected error on transition from terminal COMPENSATED")
	}
}

package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
	rules "distributed-transaction-engine/src/features/transaction-state-machine/domain/rules"
)

func TestRulesDecisionMatrix(t *testing.T) {
	if !rules.CanTransition(contracts.StateCreated, contracts.StateRunning) {
		t.Fatalf("expected CREATED -> RUNNING")
	}
	if !rules.CanTransition(contracts.StateCreated, contracts.StateCancelled) {
		t.Fatalf("expected CREATED -> CANCELLED")
	}
	if rules.CanTransition(contracts.StateCreated, contracts.StateCompleted) {
		t.Fatalf("expected CREATED -> COMPLETED to be disallowed")
	}
	if rules.CanTransition(contracts.StateCompleted, contracts.StateRunning) {
		t.Fatalf("expected COMPLETED -> RUNNING to be disallowed")
	}
	if rules.CanTransition(contracts.StateFailed, contracts.StateCompensating) {
		t.Fatalf("expected FAILED -> COMPENSATING to be disallowed")
	}
	if !rules.CanTransition(contracts.StateRunning, contracts.StateCompensating) {
		t.Fatalf("expected RUNNING -> COMPENSATING")
	}
	if !rules.CanTransition(contracts.StateCompensating, contracts.StateStuck) {
		t.Fatalf("expected COMPENSATING -> STUCK")
	}
	if !rules.CanTransition(contracts.StateStuck, contracts.StateCompensating) {
		t.Fatalf("expected STUCK -> COMPENSATING")
	}
}

func TestRulesTerminalPredicate(t *testing.T) {
	terminals := []contracts.TransactionState{
		contracts.StateCompleted,
		contracts.StateCancelled,
		contracts.StateCompensated,
		contracts.StateFailed,
	}

	for _, state := range terminals {
		if !rules.IsTerminal(state) {
			t.Errorf("expected state %s to be terminal", state)
		}
	}

	nonTerminals := []contracts.TransactionState{
		contracts.StateCreated,
		contracts.StateRunning,
		contracts.StateWaiting,
		contracts.StateThrottledWait,
		contracts.StateReconciling,
		contracts.StateCompensating,
		contracts.StateStuck,
	}

	for _, state := range nonTerminals {
		if rules.IsTerminal(state) {
			t.Errorf("expected state %s to be non-terminal", state)
		}
	}
}

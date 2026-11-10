package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/features/distributed-tracing/domain/machines"
)

func TestDeepTraceSessionMachine_Lifecycle(t *testing.T) {
	m := machines.NewDeepTraceSessionMachine()

	future := time.Now().UTC().Add(time.Hour)
	st, err := m.EvaluateSessionState(contracts.DeepSessionActive, future, 10, 1000)
	if err != nil || st != contracts.DeepSessionActive {
		t.Fatalf("expected active state, got %s err: %v", st, err)
	}

	st, err = m.EvaluateSessionState(contracts.DeepSessionActive, future, 1000, 1000)
	if err != nil || st != contracts.DeepSessionCapped {
		t.Fatalf("expected capped state when max spans reached, got %s", st)
	}

	past := time.Now().UTC().Add(-time.Hour)
	st, err = m.EvaluateSessionState(contracts.DeepSessionActive, past, 10, 1000)
	if err != nil || st != contracts.DeepSessionExpired {
		t.Fatalf("expected expired state, got %s", st)
	}

	_, err = m.EvaluateSessionState(contracts.DeepSessionCancelled, future, 10, 1000)
	if err == nil {
		t.Fatal("expected error evaluating cancelled session")
	}
}


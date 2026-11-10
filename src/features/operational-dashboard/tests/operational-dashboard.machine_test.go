package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/features/operational-dashboard/domain/machines"
)

func TestStreamSessionStateMachine_Transitions(t *testing.T) {
	sm := machines.NewStreamSessionStateMachine()

	next, err := sm.Transition(contracts.SessionStatusConnecting, machines.EventEstablished)
	if err != nil || next != contracts.SessionStatusActive {
		t.Fatalf("expected ACTIVE after ESTABLISHED, got %s err: %v", next, err)
	}

	next, err = sm.Transition(contracts.SessionStatusActive, machines.EventSourceDegraded)
	if err != nil || next != contracts.SessionStatusStalePaused {
		t.Fatalf("expected STALE_PAUSED after SOURCE_DEGRADED, got %s err: %v", next, err)
	}

	next, err = sm.Transition(contracts.SessionStatusStalePaused, machines.EventSourceRecovered)
	if err != nil || next != contracts.SessionStatusActive {
		t.Fatalf("expected ACTIVE after SOURCE_RECOVERED, got %s err: %v", next, err)
	}

	next, err = sm.Transition(contracts.SessionStatusActive, machines.EventRevocationPush)
	if err != nil || next != contracts.SessionStatusRevoked {
		t.Fatalf("expected REVOKED after REVOCATION_PUSH, got %s err: %v", next, err)
	}

	next, err = sm.Transition(contracts.SessionStatusRevoked, machines.EventDisconnect)
	if err != nil || next != contracts.SessionStatusTerminated {
		t.Fatalf("expected TERMINATED after DISCONNECT, got %s err: %v", next, err)
	}

	_, err = sm.Transition(contracts.SessionStatusTerminated, machines.EventEstablished)
	if err == nil {
		t.Fatal("expected error transitioning out of TERMINATED")
	}
}


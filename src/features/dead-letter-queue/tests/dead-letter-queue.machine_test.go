package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/machines"
)

func TestMachine_Transitions(t *testing.T) {
	m := machines.NewDeadLetterMachine()

	t.Run("quarantined to triaged", func(t *testing.T) {
		next, err := m.Transition(contracts.StatusQuarantined, machines.EventTriage)
		if err != nil || next != contracts.StatusTriaged {
			t.Fatalf("unexpected transition result: %v, %v", next, err)
		}
	})

	t.Run("quarantined to escalated", func(t *testing.T) {
		next, err := m.Transition(contracts.StatusQuarantined, machines.EventEscalate)
		if err != nil || next != contracts.StatusEscalated {
			t.Fatalf("unexpected transition result: %v, %v", next, err)
		}
	})

	t.Run("triaged to action in progress", func(t *testing.T) {
		next, err := m.Transition(contracts.StatusTriaged, machines.EventStartAction)
		if err != nil || next != contracts.StatusActionInProgress {
			t.Fatalf("unexpected transition result: %v, %v", next, err)
		}
	})

	t.Run("action in progress to resolved", func(t *testing.T) {
		next, err := m.Transition(contracts.StatusActionInProgress, machines.EventResolve)
		if err != nil || next != contracts.StatusResolved {
			t.Fatalf("unexpected transition result: %v, %v", next, err)
		}
	})

	t.Run("action in progress failure reverts to triaged", func(t *testing.T) {
		next, err := m.Transition(contracts.StatusActionInProgress, machines.EventActionFailed)
		if err != nil || next != contracts.StatusTriaged {
			t.Fatalf("unexpected transition result: %v, %v", next, err)
		}
	})

	t.Run("resolved is terminal", func(t *testing.T) {
		_, err := m.Transition(contracts.StatusResolved, machines.EventTriage)
		if err == nil {
			t.Fatalf("expected error transitioning from resolved state")
		}
	})

	t.Run("invalid transition rejected", func(t *testing.T) {
		_, err := m.Transition(contracts.StatusQuarantined, machines.EventResolve)
		if err == nil {
			t.Fatalf("expected error on invalid transition")
		}
	})
}


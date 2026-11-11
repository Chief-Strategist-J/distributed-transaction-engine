package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/compensation-retry/domain/machines"
)

func TestMachine_Transitions(t *testing.T) {
	m := machines.NewCompensationRetryMachine()

	t.Run("pending to retry scheduled", func(t *testing.T) {
		next, err := m.Transition(machines.CompensationStatePending, machines.CompensationEventScheduleRetry)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if next != machines.CompensationStateRetryScheduled {
			t.Errorf("expected RETRY_SCHEDULED, got %s", next)
		}
	})

	t.Run("pending to verifying ambiguous", func(t *testing.T) {
		next, err := m.Transition(machines.CompensationStatePending, machines.CompensationEventMarkAmbiguous)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if next != machines.CompensationStateVerifyingAmbiguous {
			t.Errorf("expected VERIFYING_AMBIGUOUS, got %s", next)
		}
	})

	t.Run("retry scheduled to compensated", func(t *testing.T) {
		next, err := m.Transition(machines.CompensationStateRetryScheduled, machines.CompensationEventSucceed)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if next != machines.CompensationStateCompensated {
			t.Errorf("expected COMPENSATED, got %s", next)
		}
	})

	t.Run("step type paused to retry scheduled on resume", func(t *testing.T) {
		next, err := m.Transition(machines.CompensationStateStepTypePaused, machines.CompensationEventScheduleRetry)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if next != machines.CompensationStateRetryScheduled {
			t.Errorf("expected RETRY_SCHEDULED, got %s", next)
		}
	})

	t.Run("terminal state cannot transition", func(t *testing.T) {
		_, err := m.Transition(machines.CompensationStateCompensated, machines.CompensationEventScheduleRetry)
		if err == nil {
			t.Fatalf("expected error from terminal COMPENSATED transition")
		}
	})
}

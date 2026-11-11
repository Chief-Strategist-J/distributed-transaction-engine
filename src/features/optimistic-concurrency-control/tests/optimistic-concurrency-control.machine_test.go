package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/machines"
)

func TestMachine_Transitions(t *testing.T) {
	m := machines.NewOCCStrategyMachine()

	t.Run("standard to warning on conflict", func(t *testing.T) {
		next, err := m.Transition(machines.StateOCCStandard, machines.EventConflictRecorded)
		if err != nil || next != machines.StateContentionWarning {
			t.Fatalf("unexpected transition: %v, %v", next, err)
		}
	})

	t.Run("warning to serialized on threshold exceeded", func(t *testing.T) {
		next, err := m.Transition(machines.StateContentionWarning, machines.EventThresholdExceeded)
		if err != nil || next != machines.StateSerializedPartition {
			t.Fatalf("unexpected transition: %v, %v", next, err)
		}
		strategy := m.MapToStrategy(next)
		if strategy != contracts.StrategySerialized {
			t.Errorf("expected StrategySerialized, got %s", strategy)
		}
	})

	t.Run("serialized to standard on contention cleared", func(t *testing.T) {
		next, err := m.Transition(machines.StateSerializedPartition, machines.EventContentionCleared)
		if err != nil || next != machines.StateOCCStandard {
			t.Fatalf("unexpected transition: %v, %v", next, err)
		}
		strategy := m.MapToStrategy(next)
		if strategy != contracts.StrategyOCC {
			t.Errorf("expected StrategyOCC, got %s", strategy)
		}
	})

	t.Run("direct force serialize", func(t *testing.T) {
		next, err := m.Transition(machines.StateOCCStandard, machines.EventForceSerialize)
		if err != nil || next != machines.StateSerializedPartition {
			t.Fatalf("unexpected transition: %v, %v", next, err)
		}
	})
}

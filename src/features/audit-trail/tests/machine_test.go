package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/audit-trail/domain/machines"
)

func TestAuditChainMachine_Transitions(t *testing.T) {
	m := machines.NewAuditChainMachine()

	if err := m.ValidateTransition(machines.SegmentStateActive, machines.SegmentStateSealed); err != nil {
		t.Fatalf("expected valid transition from active to sealed, got %v", err)
	}

	if err := m.ValidateTransition(machines.SegmentStateActive, machines.SegmentStateCompromised); err != nil {
		t.Fatalf("expected valid transition from active to compromised, got %v", err)
	}

	if err := m.ValidateTransition(machines.SegmentStateSealed, machines.SegmentStateActive); err == nil {
		t.Fatal("expected error transitioning from sealed to active")
	}

	if err := m.ValidateTransition(machines.SegmentStateCompromised, machines.SegmentStateActive); err == nil {
		t.Fatal("expected error transitioning from compromised to active")
	}
}


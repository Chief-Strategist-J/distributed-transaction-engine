package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/fencing-tokens/domain/machines"
)

func TestMachine_Transitions(t *testing.T) {
	m := machines.NewFencingLeaseMachine()

	s1, err := m.Transition(machines.LeaseStateUnallocated, machines.LeaseEventAcquire)
	if err != nil || s1 != machines.LeaseStateAcquired {
		t.Fatalf("expected UNALLOCATED -> ACQUIRED, got state=%s err=%v", s1, err)
	}

	s2, err := m.Transition(s1, machines.LeaseEventRenew)
	if err != nil || s2 != machines.LeaseStateRenewed {
		t.Fatalf("expected ACQUIRED -> RENEWED, got state=%s err=%v", s2, err)
	}

	s3, err := m.Transition(s2, machines.LeaseEventExpire)
	if err != nil || s3 != machines.LeaseStateExpired {
		t.Fatalf("expected RENEWED -> EXPIRED, got state=%s err=%v", s3, err)
	}

	s4, err := m.Transition(s3, machines.LeaseEventTakeover)
	if err != nil || s4 != machines.LeaseStateAcquired {
		t.Fatalf("expected EXPIRED -> ACQUIRED (takeover), got state=%s err=%v", s4, err)
	}

	s5, err := m.Transition(s4, machines.LeaseEventRelease)
	if err != nil || s5 != machines.LeaseStateReleased {
		t.Fatalf("expected ACQUIRED -> RELEASED, got state=%s err=%v", s5, err)
	}

	_, err = m.Transition(machines.LeaseStateReleased, machines.LeaseEventRenew)
	if err == nil {
		t.Fatalf("expected illegal transition error on RELEASED -> RENEW")
	}
}


package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/retry-policies/domain/machines"
)

func TestCircuitBreaker_Lifecycle(t *testing.T) {
	mgr := machines.NewCircuitBreakerManager()
	depID := "payment_gateway"

	if !mgr.AllowRequest(depID) {
		t.Fatalf("expected new breaker to be CLOSED and allow requests")
	}

	for i := 0; i < 4; i++ {
		mgr.RecordFailure(depID)
		if mgr.GetState(depID) != machines.BreakerClosed {
			t.Fatalf("expected breaker to remain CLOSED at failure %d", i+1)
		}
	}

	mgr.RecordFailure(depID)
	if mgr.GetState(depID) != machines.BreakerOpen {
		t.Fatalf("expected breaker to trip to OPEN on 5th failure, got %s", mgr.GetState(depID))
	}

	if mgr.AllowRequest(depID) {
		t.Fatalf("expected OPEN breaker to reject requests")
	}

	mgr.Reset(depID)
	if mgr.GetState(depID) != machines.BreakerClosed {
		t.Fatalf("expected breaker to be CLOSED after Reset, got %s", mgr.GetState(depID))
	}
	if !mgr.AllowRequest(depID) {
		t.Fatalf("expected reset breaker to allow requests")
	}
}

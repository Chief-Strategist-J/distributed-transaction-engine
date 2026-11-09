package machines

import (
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type LeaseState string

const (
	LeaseStateUnallocated LeaseState = "UNALLOCATED"
	LeaseStateAcquired    LeaseState = "ACQUIRED"
	LeaseStateRenewed     LeaseState = "RENEWED"
	LeaseStateReleased    LeaseState = "RELEASED"
	LeaseStateExpired     LeaseState = "EXPIRED"
)

type LeaseEvent string

const (
	LeaseEventAcquire  LeaseEvent = "ACQUIRE"
	LeaseEventRenew    LeaseEvent = "RENEW"
	LeaseEventRelease  LeaseEvent = "RELEASE"
	LeaseEventExpire   LeaseEvent = "EXPIRE"
	LeaseEventTakeover LeaseEvent = "TAKEOVER"
)

type FencingLeaseMachine struct{}

func NewFencingLeaseMachine() *FencingLeaseMachine {
	return &FencingLeaseMachine{}
}

func (m *FencingLeaseMachine) Transition(current LeaseState, event LeaseEvent) (LeaseState, error) {
	switch current {
	case LeaseStateUnallocated:
		if event == LeaseEventAcquire {
			return LeaseStateAcquired, nil
		}
	case LeaseStateAcquired:
		switch event {
		case LeaseEventRenew:
			return LeaseStateRenewed, nil
		case LeaseEventRelease:
			return LeaseStateReleased, nil
		case LeaseEventExpire:
			return LeaseStateExpired, nil
		}
	case LeaseStateRenewed:
		switch event {
		case LeaseEventRenew:
			return LeaseStateRenewed, nil
		case LeaseEventRelease:
			return LeaseStateReleased, nil
		case LeaseEventExpire:
			return LeaseStateExpired, nil
		}
	case LeaseStateExpired:
		if event == LeaseEventTakeover || event == LeaseEventAcquire {
			return LeaseStateAcquired, nil
		}
	case LeaseStateReleased:
		if event == LeaseEventAcquire || event == LeaseEventTakeover {
			return LeaseStateAcquired, nil
		}
	}

	return current, appErrors.NewConflictError("illegal state transition from " + string(current) + " with event " + string(event))
}

func (m *FencingLeaseMachine) CanRenew(current LeaseState) bool {
	return current == LeaseStateAcquired || current == LeaseStateRenewed
}

func (m *FencingLeaseMachine) CanRelease(current LeaseState) bool {
	return current == LeaseStateAcquired || current == LeaseStateRenewed
}

package machines

import (
	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
)

type SessionEvent string

const (
	EventEstablished     SessionEvent = "ESTABLISHED"
	EventRejected        SessionEvent = "REJECTED"
	EventSourceDegraded  SessionEvent = "SOURCE_DEGRADED"
	EventSourceRecovered SessionEvent = "SOURCE_RECOVERED"
	EventRevocationPush  SessionEvent = "REVOCATION_PUSH"
	EventDisconnect      SessionEvent = "DISCONNECT"
)

type StreamSessionStateMachine struct{}

func NewStreamSessionStateMachine() *StreamSessionStateMachine {
	return &StreamSessionStateMachine{}
}

func (m *StreamSessionStateMachine) Transition(current contracts.SessionStatus, event SessionEvent) (spanResult0 contracts.SessionStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch current {
	case contracts.SessionStatusConnecting:
		switch event {
		case EventEstablished:
			return contracts.SessionStatusActive, nil
		case EventRejected, EventDisconnect:
			return contracts.SessionStatusTerminated, nil
		default:
			return current, appErrors.NewPreconditionFailedError(fmt.Sprintf("invalid event %s in CONNECTING state", event))
		}

	case contracts.SessionStatusActive:
		switch event {
		case EventSourceDegraded:
			return contracts.SessionStatusStalePaused, nil
		case EventRevocationPush:
			return contracts.SessionStatusRevoked, nil
		case EventDisconnect:
			return contracts.SessionStatusTerminated, nil
		default:
			return current, appErrors.NewPreconditionFailedError(fmt.Sprintf("invalid event %s in ACTIVE state", event))
		}

	case contracts.SessionStatusStalePaused:
		switch event {
		case EventSourceRecovered:
			return contracts.SessionStatusActive, nil
		case EventRevocationPush:
			return contracts.SessionStatusRevoked, nil
		case EventDisconnect:
			return contracts.SessionStatusTerminated, nil
		default:
			return current, appErrors.NewPreconditionFailedError(fmt.Sprintf("invalid event %s in STALE_PAUSED state", event))
		}

	case contracts.SessionStatusRevoked:
		switch event {
		case EventDisconnect:
			return contracts.SessionStatusTerminated, nil
		default:
			return current, appErrors.NewPreconditionFailedError(fmt.Sprintf("invalid event %s in REVOKED state", event))
		}

	case contracts.SessionStatusTerminated:
		return current, appErrors.NewPreconditionFailedError("cannot transition from TERMINATED state")

	default:
		return current, appErrors.NewPreconditionFailedError(fmt.Sprintf("unknown current session status: %s", current))
	}
}

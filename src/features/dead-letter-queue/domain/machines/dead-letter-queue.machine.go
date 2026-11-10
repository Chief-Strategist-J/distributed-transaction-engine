package machines

import (
	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type RecordEvent string

const (
	EventTriage       RecordEvent = "TRIAGE"
	EventEscalate     RecordEvent = "ESCALATE"
	EventStartAction  RecordEvent = "START_ACTION"
	EventActionFailed RecordEvent = "ACTION_FAILED"
	EventResolve      RecordEvent = "RESOLVE"
	EventDeescalate   RecordEvent = "DEESCALATE"
)

type DeadLetterMachine struct{}

func NewDeadLetterMachine() *DeadLetterMachine {
	return &DeadLetterMachine{}
}

func (m *DeadLetterMachine) Transition(current contracts.RecordStatus, event RecordEvent) (spanResult0 contracts.RecordStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch current {
	case contracts.StatusQuarantined:
		switch event {
		case EventTriage:
			return contracts.StatusTriaged, nil
		case EventEscalate:
			return contracts.StatusEscalated, nil
		case EventStartAction:
			return contracts.StatusActionInProgress, nil
		}
	case contracts.StatusTriaged:
		switch event {
		case EventStartAction:
			return contracts.StatusActionInProgress, nil
		case EventEscalate:
			return contracts.StatusEscalated, nil
		}
	case contracts.StatusActionInProgress:
		switch event {
		case EventResolve:
			return contracts.StatusResolved, nil
		case EventActionFailed:
			return contracts.StatusTriaged, nil
		case EventEscalate:
			return contracts.StatusEscalated, nil
		}
	case contracts.StatusEscalated:
		switch event {
		case EventStartAction:
			return contracts.StatusActionInProgress, nil
		case EventDeescalate:
			return contracts.StatusTriaged, nil
		}
	case contracts.StatusResolved:
		return "", appErrors.NewConflictError("cannot transition from terminal state: RESOLVED")
	}

	return "", appErrors.NewConflictError("illegal state transition from " + string(current) + " on event " + string(event))
}

package machines

import (
	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type ScheduledExecutionMachine struct{}

func NewScheduledExecutionMachine() *ScheduledExecutionMachine {
	return &ScheduledExecutionMachine{}
}

func (m *ScheduledExecutionMachine) CanTransition(from contracts.ScheduleStatus, to contracts.ScheduleStatus) bool {
	defer observability.TraceEnter(nil)()
	switch from {
	case contracts.ScheduleActive:
		return to == contracts.SchedulePaused || to == contracts.ScheduleExpired || to == contracts.ScheduleCompleted
	case contracts.SchedulePaused:
		return to == contracts.ScheduleActive || to == contracts.ScheduleExpired
	default:
		return false
	}
}

func (m *ScheduledExecutionMachine) Transition(from contracts.ScheduleStatus, to contracts.ScheduleStatus) (spanResult0 contracts.ScheduleStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if !m.CanTransition(from, to) {
		return from, appErrors.NewConflictError("Illegal schedule transition from " + string(from) + " to " + string(to))
	}
	return to, nil
}

package rules

import (
	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"time"
)

type ScheduledExecutionRules struct{}

func NewScheduledExecutionRules() *ScheduledExecutionRules {
	return &ScheduledExecutionRules{}
}

func (r *ScheduledExecutionRules) ComputeNextOccurrence(schedule contracts.ScheduleRecord, from time.Time) (time.Time, bool) {
	defer observability.TraceEnter(nil)()
	loc, err := time.LoadLocation(schedule.TimeZone)
	if err != nil {
		loc = time.UTC
	}

	if schedule.ScheduleType == contracts.TypeOneOff {
		return schedule.NextOccurrenceAt, false
	}

	localFrom := from.In(loc)
	nextLocal := localFrom.Add(time.Hour)
	return nextLocal.UTC(), true
}

func (r *ScheduledExecutionRules) IsOccurrenceExpired(dueTime time.Time, now time.Time, maxLatenessSec int) bool {
	defer observability.TraceEnter(nil)()
	if maxLatenessSec <= 0 {
		return false
	}
	diff := now.Sub(dueTime).Seconds()
	return diff > float64(maxLatenessSec)
}

func (r *ScheduledExecutionRules) DecideOverlapAction(policy contracts.OverlapPolicy, isRunning bool) (bool, contracts.OccurrenceStatus) {
	defer observability.TraceEnter(nil)()
	if !isRunning {
		return true, contracts.OccurrenceDispatched
	}
	switch policy {
	case contracts.OverlapAllow:
		return true, contracts.OccurrenceDispatched
	case contracts.OverlapSkipIfRunning:
		return false, contracts.OccurrenceSkippedOverlap
	default:
		return false, contracts.OccurrenceSkippedOverlap
	}
}

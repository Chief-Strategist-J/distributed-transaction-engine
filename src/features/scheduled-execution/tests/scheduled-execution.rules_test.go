package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/rules"
)

func TestScheduledExecutionRulesNextOccurrence(t *testing.T) {
	r := rules.NewScheduledExecutionRules()
	sch := contracts.ScheduleRecord{
		ScheduleType: contracts.TypeRecurringCron,
		TimeZone:     "Asia/Kolkata",
	}

	start := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)
	next, recurring := r.ComputeNextOccurrence(sch, start)
	if !recurring {
		t.Fatalf("expected recurring schedule to return true")
	}
	if !next.After(start) {
		t.Fatalf("expected next occurrence to be strictly after start")
	}
}

func TestScheduledExecutionRulesExpiry(t *testing.T) {
	r := rules.NewScheduledExecutionRules()
	due := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)

	if !r.IsOccurrenceExpired(due, now, 3600) {
		t.Fatalf("expected occurrence 2 hours late to be expired with 1h max lateness")
	}

	if r.IsOccurrenceExpired(due, now, 10000) {
		t.Fatalf("expected occurrence to be valid within 10000s max lateness")
	}
}

func TestScheduledExecutionRulesOverlap(t *testing.T) {
	r := rules.NewScheduledExecutionRules()

	allow, status := r.DecideOverlapAction(contracts.OverlapSkipIfRunning, true)
	if allow || status != contracts.OccurrenceSkippedOverlap {
		t.Fatalf("expected skip if running")
	}

	allow, status = r.DecideOverlapAction(contracts.OverlapAllow, true)
	if !allow || status != contracts.OccurrenceDispatched {
		t.Fatalf("expected allow even when running")
	}
}

package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/rules"
)

func TestRules_CanAdmit(t *testing.T) {
	r := rules.NewDeadLetterRules()

	admissible := []contracts.FailureClass{
		contracts.FailurePoison,
		contracts.FailureDefinitionDefect,
		contracts.FailureRetryExhausted,
		contracts.FailureBusinessRejection,
		contracts.FailureCompensationFailure,
		contracts.FailureIntegrityViolation,
	}

	for _, fc := range admissible {
		if !r.CanAdmit(fc) {
			t.Errorf("expected failure class %s to be admissible", fc)
		}
	}

	if r.CanAdmit("TRANSIENT_TIMEOUT") {
		t.Errorf("expected transient failure class to be rejected")
	}
}

func TestRules_EvaluateReplaySafety(t *testing.T) {
	r := rules.NewDeadLetterRules()
	now := time.Now().UTC()
	deadline := now.Add(1 * time.Hour)

	rec := contracts.DeadLetterRecord{
		StepIndex:         1,
		Status:            contracts.StatusTriaged,
		OutcomeClass:      contracts.OutcomeNotExecuted,
		BusinessDeadline:  &deadline,
		MoneyAtStakeCents: 1000,
	}

	t.Run("safe replay passes", func(t *testing.T) {
		res := r.EvaluateReplaySafety(rec, "RUNNING", 1, false, now)
		if !res.Allowed {
			t.Errorf("expected replay allowed, got false: %s", res.Reason)
		}
	})

	t.Run("step type paused blocks replay", func(t *testing.T) {
		res := r.EvaluateReplaySafety(rec, "RUNNING", 1, true, now)
		if res.Allowed {
			t.Errorf("expected replay blocked when step type is paused")
		}
	})

	t.Run("resolved record blocks replay", func(t *testing.T) {
		resolvedRec := rec
		resolvedRec.Status = contracts.StatusResolved
		res := r.EvaluateReplaySafety(resolvedRec, "RUNNING", 1, false, now)
		if res.Allowed {
			t.Errorf("expected replay blocked for resolved record")
		}
	})

	t.Run("compensated saga blocks replay", func(t *testing.T) {
		res := r.EvaluateReplaySafety(rec, "COMPENSATED", 1, false, now)
		if res.Allowed {
			t.Errorf("expected replay blocked for compensated saga")
		}
	})

	t.Run("step mismatch blocks replay", func(t *testing.T) {
		res := r.EvaluateReplaySafety(rec, "RUNNING", 2, false, now)
		if res.Allowed {
			t.Errorf("expected replay blocked on step index mismatch")
		}
	})

	t.Run("unknown outcome requires ambiguity resolution", func(t *testing.T) {
		unknownRec := rec
		unknownRec.OutcomeClass = contracts.OutcomeUnknown
		res := r.EvaluateReplaySafety(unknownRec, "RUNNING", 1, false, now)
		if res.Allowed {
			t.Errorf("expected replay blocked for unknown outcome")
		}
		if !res.RequiresAmbiguityResolution {
			t.Errorf("expected requiresAmbiguityResolution=true")
		}
	})

	t.Run("expired deadline blocks replay", func(t *testing.T) {
		past := now.Add(-10 * time.Minute)
		expiredRec := rec
		expiredRec.BusinessDeadline = &past
		res := r.EvaluateReplaySafety(expiredRec, "RUNNING", 1, false, now)
		if res.Allowed {
			t.Errorf("expected replay blocked when deadline expired")
		}
	})
}

func TestRules_RequiresSecondApprover(t *testing.T) {
	r := rules.NewDeadLetterRules()

	if !r.RequiresSecondApprover(5000, contracts.DispositionWrittenOff) {
		t.Errorf("expected second approver required for monetary write off")
	}

	if !r.RequiresSecondApprover(5000, contracts.DispositionCorrected) {
		t.Errorf("expected second approver required for monetary corrective action")
	}

	if r.RequiresSecondApprover(0, contracts.DispositionWrittenOff) {
		t.Errorf("expected no second approver for zero money write off")
	}

	if r.RequiresSecondApprover(5000, contracts.DispositionRetriedSucceeded) {
		t.Errorf("expected no second approver for retry")
	}
}

func TestRules_IsSystemicBurst(t *testing.T) {
	r := rules.NewDeadLetterRules()

	if !r.IsSystemicBurst(20, 20) {
		t.Errorf("expected systemic burst at threshold")
	}
	if !r.IsSystemicBurst(25, 20) {
		t.Errorf("expected systemic burst above threshold")
	}
	if r.IsSystemicBurst(19, 20) {
		t.Errorf("expected no systemic burst below threshold")
	}
}

func TestRules_ShouldEscalate(t *testing.T) {
	r := rules.NewDeadLetterRules()
	now := time.Now().UTC()

	rec := contracts.DeadLetterRecord{
		Status:            contracts.StatusQuarantined,
		FailureClass:      contracts.FailureIntegrityViolation,
		MoneyAtStakeCents: 100,
	}

	if !r.ShouldEscalate(rec, now, 100000) {
		t.Errorf("expected escalation for integrity violation")
	}

	rec.FailureClass = contracts.FailureRetryExhausted
	rec.MoneyAtStakeCents = 150000
	if !r.ShouldEscalate(rec, now, 100000) {
		t.Errorf("expected escalation for high money threshold")
	}

	rec.MoneyAtStakeCents = 1000
	nearDeadline := now.Add(1 * time.Hour)
	rec.BusinessDeadline = &nearDeadline
	if !r.ShouldEscalate(rec, now, 100000) {
		t.Errorf("expected escalation when deadline within 2 hours")
	}
}


package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"distributed-transaction-engine/src/features/compensation-retry/domain/rules"
)

func TestRules_ClassifyResult(t *testing.T) {
	r := rules.NewCompensationRetryRules()

	t.Run("classifies 200 as done", func(t *testing.T) {
		outcome := r.ClassifyResult(200, "")
		if outcome != contracts.OutcomeDone {
			t.Errorf("expected OutcomeDone, got %s", outcome)
		}
	})

	t.Run("classifies 504 as ambiguous", func(t *testing.T) {
		outcome := r.ClassifyResult(504, "gateway timeout")
		if outcome != contracts.OutcomeAmbiguous {
			t.Errorf("expected OutcomeAmbiguous, got %s", outcome)
		}
	})

	t.Run("classifies timeout error as ambiguous", func(t *testing.T) {
		outcome := r.ClassifyResult(0, "connection timeout")
		if outcome != contracts.OutcomeAmbiguous {
			t.Errorf("expected OutcomeAmbiguous, got %s", outcome)
		}
	})

	t.Run("classifies 400 as contract defect", func(t *testing.T) {
		outcome := r.ClassifyResult(400, "invalid payload schema")
		if outcome != contracts.OutcomeContractDefect {
			t.Errorf("expected OutcomeContractDefect, got %s", outcome)
		}
	})

	t.Run("classifies 401 as credential defect", func(t *testing.T) {
		outcome := r.ClassifyResult(401, "unauthorized")
		if outcome != contracts.OutcomeCredential {
			t.Errorf("expected OutcomeCredential, got %s", outcome)
		}
	})

	t.Run("classifies 404 as business alternative", func(t *testing.T) {
		outcome := r.ClassifyResult(404, "order already shipped")
		if outcome != contracts.OutcomeBusinessAlternative {
			t.Errorf("expected OutcomeBusinessAlternative, got %s", outcome)
		}
	})

	t.Run("classifies 429 as rate limited", func(t *testing.T) {
		outcome := r.ClassifyResult(429, "too many requests")
		if outcome != contracts.OutcomeRateLimited {
			t.Errorf("expected OutcomeRateLimited, got %s", outcome)
		}
	})

	t.Run("classifies 503 as retry", func(t *testing.T) {
		outcome := r.ClassifyResult(503, "service unavailable")
		if outcome != contracts.OutcomeRetry {
			t.Errorf("expected OutcomeRetry, got %s", outcome)
		}
	})
}

func TestRules_CalculateBackoff(t *testing.T) {
	r := rules.NewCompensationRetryRules()

	for attempt := 1; attempt <= 10; attempt++ {
		backoff := r.CalculateBackoff(attempt, 500, 30000, 0)
		if backoff < 250*time.Millisecond {
			t.Errorf("backoff below minimum floor for attempt %d: %v", attempt, backoff)
		}
		if backoff > 30*time.Second {
			t.Errorf("backoff exceeds max ceiling for attempt %d: %v", attempt, backoff)
		}
	}

	retryAfterVal := 5 * time.Second
	fixed := r.CalculateBackoff(1, 500, 30000, retryAfterVal)
	if fixed != retryAfterVal {
		t.Errorf("expected retry-after header to take precedence, got %v", fixed)
	}
}

func TestRules_DetermineEscalation(t *testing.T) {
	r := rules.NewCompensationRetryRules()
	now := time.Now().UTC()
	first := now.Add(-10 * time.Minute)

	t.Run("none when plenty of time remaining", func(t *testing.T) {
		deadline := now.Add(2 * time.Hour)
		esc, exhausted := r.DetermineEscalation(now, first, deadline, 2, 10)
		if exhausted {
			t.Errorf("expected not exhausted")
		}
		if esc != contracts.EscalationNone {
			t.Errorf("expected EscalationNone, got %s", esc)
		}
	})

	t.Run("missed when deadline passed", func(t *testing.T) {
		deadline := now.Add(-1 * time.Minute)
		esc, exhausted := r.DetermineEscalation(now, first, deadline, 3, 10)
		if !exhausted {
			t.Errorf("expected exhausted=true")
		}
		if esc != contracts.EscalationLevel3Missed {
			t.Errorf("expected EscalationLevel3Missed, got %s", esc)
		}
	})

	t.Run("deadline risk when attempts exhausted", func(t *testing.T) {
		deadline := now.Add(1 * time.Hour)
		esc, exhausted := r.DetermineEscalation(now, first, deadline, 10, 10)
		if !exhausted {
			t.Errorf("expected exhausted=true")
		}
		if esc != contracts.EscalationLevel2Deadline {
			t.Errorf("expected EscalationLevel2Deadline, got %s", esc)
		}
	})
}

func TestRules_BackoffIsFullyJitteredWithinItsBounds(t *testing.T) {
	r := rules.NewCompensationRetryRules()
	const attempt, base, max = 5, 1000, 30000
	ceiling := time.Duration(base*(1<<(attempt-1))) * time.Millisecond
	floor := time.Duration(base/2) * time.Millisecond
	distinct := make(map[time.Duration]struct{})
	var low, high time.Duration = ceiling, 0
	for i := 0; i < 500; i++ {
		backoff := r.CalculateBackoff(attempt, base, max, 0)
		if backoff < floor || backoff >= ceiling {
			t.Fatalf("backoff %v escaped [%v, %v)", backoff, floor, ceiling)
		}
		distinct[backoff] = struct{}{}
		if backoff < low {
			low = backoff
		}
		if backoff > high {
			high = backoff
		}
	}
	if len(distinct) < 100 {
		t.Fatalf("a thundering herd needs dispersion, got only %d distinct delays in 500 draws", len(distinct))
	}
	if high-low < (ceiling-floor)/2 {
		t.Fatalf("delays must spread across the window, observed spread %v of %v", high-low, ceiling-floor)
	}
}

func TestRules_RetryAfterOverridesJitter(t *testing.T) {
	r := rules.NewCompensationRetryRules()
	if got := r.CalculateBackoff(3, 1000, 30000, 45*time.Second); got != 45*time.Second {
		t.Fatalf("an explicit Retry-After must win over computed backoff, got %v", got)
	}
}

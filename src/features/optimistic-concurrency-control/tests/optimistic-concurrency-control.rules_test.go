package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/rules"
)

func TestRules_DiagnoseZeroRows(t *testing.T) {
	r := rules.NewOCCRules()

	t.Run("missing row diagnosed as DATA_ERROR", func(t *testing.T) {
		cause := r.DiagnoseZeroRows(false, "t1", "t1", 1, 1, 1, 1, "RUNNING")
		if cause != contracts.CauseDataError {
			t.Errorf("expected DATA_ERROR, got %s", cause)
		}
	})

	t.Run("tenant mismatch diagnosed as SECURITY_INTEGRITY_VIOLATION", func(t *testing.T) {
		cause := r.DiagnoseZeroRows(true, "t1", "t2", 1, 1, 1, 1, "RUNNING")
		if cause != contracts.CauseSecurityViolation {
			t.Errorf("expected SECURITY_INTEGRITY_VIOLATION, got %s", cause)
		}
	})

	t.Run("epoch mismatch diagnosed as NOT_OWNER", func(t *testing.T) {
		cause := r.DiagnoseZeroRows(true, "t1", "t1", 1, 1, 1, 2, "RUNNING")
		if cause != contracts.CauseNotOwner {
			t.Errorf("expected NOT_OWNER, got %s", cause)
		}
	})

	t.Run("version mismatch diagnosed as VERSION_CONFLICT", func(t *testing.T) {
		cause := r.DiagnoseZeroRows(true, "t1", "t1", 1, 2, 1, 1, "RUNNING")
		if cause != contracts.CauseVersionConflict {
			t.Errorf("expected VERSION_CONFLICT, got %s", cause)
		}
	})

	t.Run("state mismatch diagnosed as STATE_CONFLICT", func(t *testing.T) {
		cause := r.DiagnoseZeroRows(true, "t1", "t1", 1, 1, 1, 1, "COMPLETED")
		if cause != contracts.CauseStateConflict {
			t.Errorf("expected STATE_CONFLICT, got %s", cause)
		}
	})
}

func TestRules_CanRetryConflict(t *testing.T) {
	r := rules.NewOCCRules()

	if !r.CanRetryConflict(contracts.CauseVersionConflict, 1, 5) {
		t.Errorf("expected retry allowed for version conflict below max")
	}

	if r.CanRetryConflict(contracts.CauseVersionConflict, 5, 5) {
		t.Errorf("expected retry blocked when attempt equals max")
	}

	if r.CanRetryConflict(contracts.CauseNotOwner, 1, 5) {
		t.Errorf("expected retry blocked for NOT_OWNER")
	}

	if r.CanRetryConflict(contracts.CauseDataError, 1, 5) {
		t.Errorf("expected retry blocked for DATA_ERROR")
	}
}

func TestRules_ComputeJitteredBackoff(t *testing.T) {
	r := rules.NewOCCRules()

	for attempt := 0; attempt < 8; attempt++ {
		backoff := r.ComputeJitteredBackoff(attempt)
		if backoff < 5*time.Millisecond {
			t.Errorf("backoff %v below 5ms floor", backoff)
		}
		if backoff > 200*time.Millisecond {
			t.Errorf("backoff %v above 200ms ceiling", backoff)
		}
	}
}

func TestRules_IsInputAlreadyApplied(t *testing.T) {
	r := rules.NewOCCRules()
	registry := map[string]bool{
		"applied-input-1": true,
	}

	if !r.IsInputAlreadyApplied("applied-input-1", registry) {
		t.Errorf("expected true for applied input")
	}
	if r.IsInputAlreadyApplied("unseen-input-2", registry) {
		t.Errorf("expected false for unseen input")
	}
}

func TestRules_ShouldSerializeSaga(t *testing.T) {
	r := rules.NewOCCRules()

	if !r.ShouldSerializeSaga(3, 3) {
		t.Errorf("expected serialization triggered at threshold")
	}
	if !r.ShouldSerializeSaga(5, 3) {
		t.Errorf("expected serialization triggered above threshold")
	}
	if r.ShouldSerializeSaga(2, 3) {
		t.Errorf("expected no serialization below threshold")
	}
}

func TestRules_BackoffGrowsExponentiallyAndIsRandomized(t *testing.T) {
	r := rules.NewOCCRules()
	mean := func(attempt int) (float64, map[time.Duration]struct{}) {
		distinct := make(map[time.Duration]struct{})
		var total time.Duration
		const samples = 400
		for i := 0; i < samples; i++ {
			backoff := r.ComputeJitteredBackoff(attempt)
			distinct[backoff] = struct{}{}
			total += backoff
		}
		return float64(total) / samples, distinct
	}
	first, _ := mean(0)
	third, _ := mean(2)
	if third <= first {
		t.Fatalf("backoff must grow with the attempt number, got %v then %v", first, third)
	}
	capped, distinct := mean(12)
	if capped > float64(200*time.Millisecond) {
		t.Fatalf("backoff must respect its cap, mean %v", capped)
	}
	if len(distinct) < 10 {
		t.Fatalf("backoff must be randomized to avoid collision waves, only %d distinct values", len(distinct))
	}
}

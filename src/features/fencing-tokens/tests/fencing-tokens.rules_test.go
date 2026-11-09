package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/fencing-tokens/domain/rules"
)

func TestRules_EvaluateFencingBarrier(t *testing.T) {
	r := rules.NewFencingRules()

	t.Run("equal token passes barrier", func(t *testing.T) {
		decision := r.EvaluateFencingBarrier(42, 42)
		if !decision.Allowed {
			t.Errorf("expected allowed=true for equal token, got false")
		}
		if decision.AttemptedToken != 42 || decision.StoredToken != 42 {
			t.Errorf("unexpected tokens: attempted=%d, stored=%d", decision.AttemptedToken, decision.StoredToken)
		}
	})

	t.Run("higher token passes barrier", func(t *testing.T) {
		decision := r.EvaluateFencingBarrier(42, 43)
		if !decision.Allowed {
			t.Errorf("expected allowed=true for higher token, got false")
		}
	})

	t.Run("lower token rejected with STALE_TOKEN", func(t *testing.T) {
		decision := r.EvaluateFencingBarrier(43, 42)
		if decision.Allowed {
			t.Errorf("expected allowed=false for lower token, got true")
		}
		if decision.ViolationType != "STALE_TOKEN" {
			t.Errorf("expected violationType 'STALE_TOKEN', got '%s'", decision.ViolationType)
		}
	})
}

func TestRules_ComputeLeaseExpiry(t *testing.T) {
	r := rules.NewFencingRules()
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

	expiry := r.ComputeLeaseExpiry(now, 5000)
	expected := now.Add(5 * time.Second)
	if !expiry.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, expiry)
	}

	expiryDefault := r.ComputeLeaseExpiry(now, 500)
	expectedDefault := now.Add(15 * time.Second)
	if !expiryDefault.Equal(expectedDefault) {
		t.Errorf("expected default 15s (%v), got %v", expectedDefault, expiryDefault)
	}
}

func TestRules_HeadroomAndExpiry(t *testing.T) {
	r := rules.NewFencingRules()
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(10 * time.Second)

	if r.IsLeaseExpired(now, expiresAt) {
		t.Errorf("expected lease not expired")
	}

	expiredAt := now.Add(-1 * time.Second)
	if !r.IsLeaseExpired(now, expiredAt) {
		t.Errorf("expected lease expired")
	}

	if !r.HasSufficientHeadroom(now, expiresAt, 3000) {
		t.Errorf("expected sufficient headroom for 3s action within 10s lease")
	}

	if r.HasSufficientHeadroom(now, expiresAt, 6000) {
		t.Errorf("expected insufficient headroom for 6s action (requires 12s) within 10s lease")
	}
}


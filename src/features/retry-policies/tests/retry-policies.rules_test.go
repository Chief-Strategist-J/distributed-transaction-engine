package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
	"distributed-transaction-engine/src/features/retry-policies/domain/rules"
)

func TestRules_ClassifyError(t *testing.T) {
	if c := rules.ClassifyError(503, "Service Unavailable", 1, nil); c != contracts.ClassTransientNotExecuted {
		t.Fatalf("expected TRANSIENT_NOT_EXECUTED for 503, got %s", c)
	}

	if c := rules.ClassifyError(429, "Too Many Requests", 1, nil); c != contracts.ClassTransientNotExecuted {
		t.Fatalf("expected TRANSIENT_NOT_EXECUTED for 429, got %s", c)
	}

	if c := rules.ClassifyError(401, "Unauthorized", 1, nil); c != contracts.ClassCredential {
		t.Fatalf("expected CREDENTIAL for 401, got %s", c)
	}

	if c := rules.ClassifyError(404, "Not Found", 1, nil); c != contracts.ClassCausalWait {
		t.Fatalf("expected CAUSAL_WAIT for 404, got %s", c)
	}

	if c := rules.ClassifyError(400, "Bad Request", 1, nil); c != contracts.ClassRejected {
		t.Fatalf("expected REJECTED for 400, got %s", c)
	}

	if c := rules.ClassifyError(422, "Unprocessable Entity", 1, nil); c != contracts.ClassRejected {
		t.Fatalf("expected REJECTED for 422, got %s", c)
	}

	if c := rules.ClassifyError(500, "Internal Server Error", 1, nil); c != contracts.ClassAmbiguous {
		t.Fatalf("expected AMBIGUOUS for 500, got %s", c)
	}

	if c := rules.ClassifyError(504, "Gateway Timeout", 1, nil); c != contracts.ClassAmbiguous {
		t.Fatalf("expected AMBIGUOUS for 504, got %s", c)
	}

	if c := rules.ClassifyError(0, "dial tcp: connection refused", 1, nil); c != contracts.ClassNotSent {
		t.Fatalf("expected NOT_SENT for connection refused, got %s", c)
	}

	if c := rules.ClassifyError(409, "Conflict", 2, nil); c != contracts.ClassTransientNotExecuted {
		t.Fatalf("expected TRANSIENT_NOT_EXECUTED for 409 on attempt > 1, got %s", c)
	}

	if c := rules.ClassifyError(409, "Conflict", 1, nil); c != contracts.ClassRejected {
		t.Fatalf("expected REJECTED for 409 on attempt 1, got %s", c)
	}
}

func TestRules_DeterministicBugDetection(t *testing.T) {
	fp1 := rules.ComputeFingerprint(500, "NullPointerException at StepExecutor.java:42")
	history := []string{fp1, fp1}

	c := rules.ClassifyError(500, "NullPointerException at StepExecutor.java:42", 3, history)
	if c != contracts.ClassDeterministicError {
		t.Fatalf("expected DETERMINISTIC_ERROR on 3rd identical failure, got %s", c)
	}
}

func TestRules_BackoffBoundsAndOverflowClamping(t *testing.T) {
	policy := contracts.RetryPolicyConfig{
		BaseIntervalMillis: 200,
		MaxIntervalMillis:  10000,
		Algorithm:          contracts.AlgorithmFullJitter,
	}

	delayAttempt1 := rules.CalculateBackoff(1, policy, 0, false)
	if delayAttempt1 < 100*time.Millisecond || delayAttempt1 > 10000*time.Millisecond {
		t.Fatalf("delay out of bounds: %v", delayAttempt1)
	}

	delayAttempt64 := rules.CalculateBackoff(64, policy, 0, false)
	if delayAttempt64 <= 0 || delayAttempt64 > 10000*time.Millisecond {
		t.Fatalf("delay at attempt 64 overflowed: %v", delayAttempt64)
	}

	retryAfterDelay := 5 * time.Second
	delayWithHeader := rules.CalculateBackoff(1, policy, retryAfterDelay, true)
	if delayWithHeader < retryAfterDelay {
		t.Fatalf("expected backoff to honor Retry-After %v, got %v", retryAfterDelay, delayWithHeader)
	}
}

func TestRules_EvaluateDecision(t *testing.T) {
	now := time.Now().UTC()
	policy := contracts.RetryPolicyConfig{
		MaxAttempts:        3,
		MaxElapsedSeconds:  60,
		BaseIntervalMillis: 200,
		MaxIntervalMillis:  5000,
		Algorithm:          contracts.AlgorithmFullJitter,
	}

	capIdempotent := contracts.StepCapability{IdempotentByKey: true}
	decision := rules.EvaluateDecision(
		contracts.ClassTransientNotExecuted,
		1,
		now,
		nil,
		capIdempotent,
		contracts.PositionCompensable,
		policy,
		500*time.Millisecond,
		now,
	)
	if decision.Action != contracts.ActionRetry {
		t.Fatalf("expected RETRY on attempt 1, got %s", decision.Action)
	}

	decisionExhausted := rules.EvaluateDecision(
		contracts.ClassTransientNotExecuted,
		3,
		now,
		nil,
		capIdempotent,
		contracts.PositionCompensable,
		policy,
		500*time.Millisecond,
		now,
	)
	if decisionExhausted.Action != contracts.ActionHaltCompensate {
		t.Fatalf("expected HALT_COMPENSATE on budget exhaustion for compensable step, got %s", decisionExhausted.Action)
	}

	decisionPostPivotExhausted := rules.EvaluateDecision(
		contracts.ClassTransientNotExecuted,
		3,
		now,
		nil,
		capIdempotent,
		contracts.PositionRetriable,
		policy,
		500*time.Millisecond,
		now,
	)
	if decisionPostPivotExhausted.Action != contracts.ActionNeedsAttention {
		t.Fatalf("expected NEEDS_ATTENTION on budget exhaustion for post-pivot step, got %s", decisionPostPivotExhausted.Action)
	}

	capNonIdempotent := contracts.StepCapability{IdempotentByKey: false, StatusQuery: false}
	decisionAmbiguousNonIdempotent := rules.EvaluateDecision(
		contracts.ClassAmbiguous,
		1,
		now,
		nil,
		capNonIdempotent,
		contracts.PositionCompensable,
		policy,
		500*time.Millisecond,
		now,
	)
	if decisionAmbiguousNonIdempotent.Action != contracts.ActionNeedsAttention {
		t.Fatalf("expected NEEDS_ATTENTION on ambiguous timeout without status query, got %s", decisionAmbiguousNonIdempotent.Action)
	}
}


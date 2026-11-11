package tests

import (
	"context"
	"net/http"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
	"distributed-transaction-engine/src/features/retry-policies/infrastructure/adapters"
	"distributed-transaction-engine/src/features/retry-policies/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

func TestParseRetryAfterSupportsSecondsAndHTTPDates(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	future := now.Add(90 * time.Second).UTC().Format(http.TimeFormat)
	past := now.Add(-90 * time.Second).UTC().Format(http.TimeFormat)
	cases := []struct {
		header string
		want   time.Duration
		ok     bool
	}{
		{"120", 120 * time.Second, true},
		{" 7 ", 7 * time.Second, true},
		{"0", 0, true},
		{future, 90 * time.Second, true},
		{past, 0, false},
		{"-5", 0, false},
		{"soon", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := contracts.ParseRetryAfter(tc.header, now)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("ParseRetryAfter(%q) = %v,%v want %v,%v", tc.header, got, ok, tc.want, tc.ok)
		}
	}
}

func TestRateLimitedResponsesScheduleTheRetryAccordingToRetryAfter(t *testing.T) {
	svc := service.NewService(service.ServiceDeps{Repo: adapters.NewMemoryRetryRepositoryAdapter(), Tracer: observability.NewTracer("retry-after-test")})
	now := time.Now().UTC()
	base := contracts.EvaluateRetryInput{
		TenantID: "tenant_default", SagaID: uuid.New().String(), StepID: "step_charge", AttemptNumber: 1, StatusCode: 429,
		ErrorMessage: "Too Many Requests", Capability: contracts.StepCapability{IdempotentByKey: true, StatusQuery: true, EffectType: "HOLD"},
		Position: contracts.PositionCompensable, DependencyID: "payments_api", FirstAttemptTime: now.Format(time.RFC3339),
		Policy: contracts.RetryPolicyConfig{MaxAttempts: 5, MaxElapsedSeconds: 3600, BaseIntervalMillis: 200, MaxIntervalMillis: 10000, Algorithm: contracts.AlgorithmFullJitter},
	}
	for name, header := range map[string]string{
		"seconds":   "120",
		"http date": now.Add(2 * time.Minute).UTC().Format(http.TimeFormat),
	} {
		input := base
		input.RetryAfterHeader = header
		decision, err := svc.Evaluate(context.Background(), input)
		if err != nil || decision.Action != contracts.ActionRetry {
			t.Fatalf("%s: expected a retry, got %+v %v", name, decision, err)
		}
		if decision.Delay < 110*time.Second {
			t.Fatalf("%s: the retry must wait for Retry-After (~2 minutes), got %v", name, decision.Delay)
		}
	}
	input := base
	input.RetryAfterHeader = ""
	decision, err := svc.Evaluate(context.Background(), input)
	if err != nil || decision.Delay >= 110*time.Second {
		t.Fatalf("without a header the policy backoff applies, got %+v %v", decision, err)
	}
}

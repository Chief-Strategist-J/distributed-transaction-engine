package tests

import (
	"errors"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	rules "distributed-transaction-engine/src/features/transactional-outbox/domain/rules"
)

func TestClassifyPublishError(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected rules.ErrorClassification
	}{
		{
			name:     "nil error is retriable",
			err:      nil,
			expected: rules.ErrorRetriable,
		},
		{
			name:     "network timeout is retriable",
			err:      errors.New("connection reset by peer: timeout"),
			expected: rules.ErrorRetriable,
		},
		{
			name:     "broker unavailable is retriable",
			err:      errors.New("kafka broker unavailable"),
			expected: rules.ErrorRetriable,
		},
		{
			name:     "record too large is non-retriable",
			err:      errors.New("kafka: message rejected, record too large"),
			expected: rules.ErrorNonRetriable,
		},
		{
			name:     "unknown topic is retriable while the topic is provisioned",
			err:      errors.New("kafka: unknown topic or partition"),
			expected: rules.ErrorRetriable,
		},
		{
			name:     "authorization failure is non-retriable",
			err:      errors.New("kafka: authorization failed for principal"),
			expected: rules.ErrorNonRetriable,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := rules.ClassifyPublishError(tc.err)
			if res != tc.expected {
				t.Fatalf("expected classification %s, got %s", tc.expected, res)
			}
		})
	}
}

func TestValidateSequenceProgression(t *testing.T) {
	if !rules.ValidateSequenceProgression(1, 2) {
		t.Fatal("expected 1 -> 2 sequence progression to be valid")
	}
	if rules.ValidateSequenceProgression(1, 3) {
		t.Fatal("expected 1 -> 3 gap to be invalid")
	}
	if rules.ValidateSequenceProgression(2, 2) {
		t.Fatal("expected duplicate sequence 2 -> 2 to be invalid")
	}
}

func TestCanTransitionOutboxStatus(t *testing.T) {
	if !rules.CanTransitionOutboxStatus(contracts.OutboxStatusPending, contracts.OutboxStatusInFlight) {
		t.Fatal("expected PENDING -> IN_FLIGHT to be valid")
	}
	if !rules.CanTransitionOutboxStatus(contracts.OutboxStatusInFlight, contracts.OutboxStatusPublished) {
		t.Fatal("expected IN_FLIGHT -> PUBLISHED to be valid")
	}
	if !rules.CanTransitionOutboxStatus(contracts.OutboxStatusInFlight, contracts.OutboxStatusParked) {
		t.Fatal("expected IN_FLIGHT -> PARKED to be valid")
	}
	if rules.CanTransitionOutboxStatus(contracts.OutboxStatusPublished, contracts.OutboxStatusInFlight) {
		t.Fatal("expected PUBLISHED -> IN_FLIGHT to be invalid (immutable terminal state)")
	}
}

func TestRelayBackoff_DoublesAndCaps(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{-3, time.Second}, {0, time.Second}, {1, 2 * time.Second}, {2, 4 * time.Second}, {3, 8 * time.Second},
		{5, 32 * time.Second}, {6, 60 * time.Second}, {7, 60 * time.Second}, {1000, 60 * time.Second},
	}
	for _, tc := range cases {
		if got := rules.RelayBackoff(tc.attempt); got != tc.want {
			t.Fatalf("attempt %d expected %v got %v", tc.attempt, tc.want, got)
		}
	}
}

func TestExceededPublishAttempts_Boundary(t *testing.T) {
	for attempts, want := range map[int]bool{0: false, 1: false, rules.MaxPublishAttempts - 1: false, rules.MaxPublishAttempts: true, rules.MaxPublishAttempts + 1: true} {
		if got := rules.ExceededPublishAttempts(attempts); got != want {
			t.Fatalf("attempts %d expected %v got %v", attempts, want, got)
		}
	}
}

package tests

import (
	"errors"
	"testing"

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
			name:     "unknown topic is non-retriable",
			err:      errors.New("kafka: unknown topic or partition"),
			expected: rules.ErrorNonRetriable,
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

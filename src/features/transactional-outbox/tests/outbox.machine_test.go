package tests

import (
	"errors"
	"testing"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	rules "distributed-transaction-engine/src/features/transactional-outbox/domain/rules"
)

func TestOutboxStateMachine_EvaluatePublishOutcome(t *testing.T) {
	transient := errors.New("kafka: connection reset")
	unknownTopic := errors.New("failed to produce: Unknown Topic Or Partition: the request is for a topic that does not exist")
	cases := []struct {
		name     string
		status   contracts.OutboxStatus
		attempts int
		err      error
		want     contracts.OutboxStatus
		wantErr  bool
	}{
		{"success", contracts.OutboxStatusInFlight, 1, nil, contracts.OutboxStatusPublished, false},
		{"transient_first_attempt", contracts.OutboxStatusInFlight, 1, transient, contracts.OutboxStatusPending, false},
		{"transient_last_allowed_attempt", contracts.OutboxStatusInFlight, rules.MaxPublishAttempts - 1, transient, contracts.OutboxStatusPending, false},
		{"transient_exhausted", contracts.OutboxStatusInFlight, rules.MaxPublishAttempts, transient, contracts.OutboxStatusParked, false},
		{"transient_far_beyond_limit", contracts.OutboxStatusInFlight, 1000, transient, contracts.OutboxStatusParked, false},
		{"unknown_topic_is_retried", contracts.OutboxStatusInFlight, 1, unknownTopic, contracts.OutboxStatusPending, false},
		{"unknown_topic_exhausted", contracts.OutboxStatusInFlight, rules.MaxPublishAttempts, unknownTopic, contracts.OutboxStatusParked, false},
		{"record_too_large_parks_immediately", contracts.OutboxStatusInFlight, 1, errors.New("kafka: record too large"), contracts.OutboxStatusParked, false},
		{"already_published_rejects", contracts.OutboxStatusPublished, 1, nil, contracts.OutboxStatusPublished, true},
	}
	machine := machines.NewOutboxStateMachine()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := machine.EvaluatePublishOutcome(tc.status, tc.attempts, tc.err)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("expected %s (err=%v), got %s err=%v", tc.want, tc.wantErr, got, err)
			}
		})
	}
}

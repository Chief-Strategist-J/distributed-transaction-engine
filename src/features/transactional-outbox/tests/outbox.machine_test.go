package tests

import (
	"errors"
	"testing"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
)

func TestOutboxStateMachine_EvaluatePublishOutcome(t *testing.T) {
	m := machines.NewOutboxStateMachine()

	next, err := m.EvaluatePublishOutcome(contracts.OutboxStatusInFlight, nil)
	if err != nil || next != contracts.OutboxStatusPublished {
		t.Fatalf("expected transition to PUBLISHED, got %s (err: %v)", next, err)
	}

	next, err = m.EvaluatePublishOutcome(contracts.OutboxStatusInFlight, errors.New("kafka: connection reset"))
	if err != nil || next != contracts.OutboxStatusPending {
		t.Fatalf("expected transient error to transition to PENDING, got %s (err: %v)", next, err)
	}

	next, err = m.EvaluatePublishOutcome(contracts.OutboxStatusInFlight, errors.New("kafka: record too large"))
	if err != nil || next != contracts.OutboxStatusParked {
		t.Fatalf("expected non-retriable error to transition to PARKED, got %s (err: %v)", next, err)
	}

	_, err = m.EvaluatePublishOutcome(contracts.OutboxStatusPublished, nil)
	if err == nil {
		t.Fatal("expected error transitioning from terminal PUBLISHED status, got nil")
	}
}

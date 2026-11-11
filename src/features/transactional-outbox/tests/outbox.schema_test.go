package tests

import (
	stderrors "errors"
	"strings"
	"testing"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	apperrors "distributed-transaction-engine/src/shared/errors"
)

const (
	schemaSagaID  = "0192f0a0-7c3b-7a52-9d6e-3f1c2b4a5d60"
	schemaEventID = "0192f0a0-7c3b-7a52-9d6e-3f1c2b4a5d61"
	otherSagaID   = "0192f0a0-7c3b-7a52-9d6e-3f1c2b4a5d62"
)

type partitionKeyCase struct {
	name      string
	sagaID    string
	key       string
	wantIssue string
}

func validOutboxInput(sagaID string, key string) *contracts.CreateOutboxEventInput {
	return &contracts.CreateOutboxEventInput{
		ID: schemaEventID, PublicID: "oxe_" + strings.Repeat("a", 26), TenantID: "tenant_pk",
		SagaID: sagaID, AggregateType: "SAGA", AggregateID: sagaID, AggregateSequence: 1,
		DestinationTopic: "dist-tx.events.v1", PartitionKey: key, EventType: "SAGA_CREATED",
		Payload: map[string]interface{}{"k": "v"},
	}
}

func assertPartitionKeyOutcome(t *testing.T, err error, wantIssue string) {
	t.Helper()
	if wantIssue == "" {
		if err != nil {
			t.Fatalf("expected acceptance, got %v", err)
		}
		return
	}
	var appErr *apperrors.ApplicationError
	if !stderrors.As(err, &appErr) || appErr.StatusCode != 400 || !strings.Contains(appErr.Message, wantIssue) {
		t.Fatalf("expected 400 containing %q, got %#v", wantIssue, err)
	}
}

func TestSchema_PartitionKeyMustEqualSagaID(t *testing.T) {
	cases := []partitionKeyCase{
		{"saga_id_key", schemaSagaID, schemaSagaID, ""},
		{"uppercase_saga_lowercase_key", strings.ToUpper(schemaSagaID), schemaSagaID, ""},
		{"empty", schemaSagaID, "", "must not be empty"},
		{"spaces_only", schemaSagaID, "   ", "must not be empty"},
		{"tab_only", schemaSagaID, "\t", "must not be empty"},
		{"newline_only", schemaSagaID, "\n", "must not be empty"},
		{"other_saga", schemaSagaID, otherSagaID, "must equal sagaId"},
		{"business_key", schemaSagaID, "order-123", "must equal sagaId"},
		{"trailing_space", schemaSagaID, schemaSagaID + " ", "must equal sagaId"},
		{"uppercase_key", schemaSagaID, strings.ToUpper(schemaSagaID), "must equal sagaId"},
		{"prefixed_key", schemaSagaID, "saga:" + schemaSagaID, "must equal sagaId"},
		{"truncated_key", schemaSagaID, schemaSagaID[:8], "must equal sagaId"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := contracts.ValidateCreateOutboxEventInput(validOutboxInput(tc.sagaID, tc.key))
			assertPartitionKeyOutcome(t, err, tc.wantIssue)
		})
	}
}

func TestSchema_ValidatePartitionKeyDirect(t *testing.T) {
	cases := []partitionKeyCase{
		{"match", schemaSagaID, schemaSagaID, ""},
		{"empty_saga_and_key", "", "", "must not be empty"},
		{"empty_saga_nonempty_key", "", "k", "must equal sagaId"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertPartitionKeyOutcome(t, contracts.ValidatePartitionKey(tc.sagaID, tc.key), tc.wantIssue)
		})
	}
}

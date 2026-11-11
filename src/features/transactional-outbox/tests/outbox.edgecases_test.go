package tests

import (
	"context"
	"testing"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	adapters "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/adapters"
)

func memoryEvent(key string, sequence int64) *contracts.OutboxEventInstance {
	return &contracts.OutboxEventInstance{
		ID: schemaEventID, TenantID: "tenant_pk", SagaID: schemaSagaID, AggregateID: schemaSagaID,
		AggregateSequence: sequence, PartitionKey: key,
	}
}

func TestEdgeCases_MemoryAdapterRejectsInvalidPartitionKeys(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		valid bool
	}{
		{"saga_id", schemaSagaID, true},
		{"empty", "", false},
		{"blank", "   ", false},
		{"other_saga", otherSagaID, false},
		{"business_key", "order-1", false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := adapters.NewMemoryOutboxRepositoryAdapter()
			err := adapter.InsertOutboxEventTx(context.Background(), nil, memoryEvent(tc.key, int64(i+1)))
			if (err == nil) != tc.valid {
				t.Fatalf("key %q valid=%v got err=%v", tc.key, tc.valid, err)
			}
		})
	}
}

func TestEdgeCases_RejectedInsertLeavesNoEvent(t *testing.T) {
	adapter := adapters.NewMemoryOutboxRepositoryAdapter()
	if err := adapter.InsertOutboxEventTx(context.Background(), nil, memoryEvent("", 1)); err == nil {
		t.Fatal("expected rejection")
	}
	if err := adapter.InsertOutboxEventTx(context.Background(), nil, memoryEvent(schemaSagaID, 1)); err != nil {
		t.Fatalf("rejected insert must not consume the aggregate sequence: %v", err)
	}
}

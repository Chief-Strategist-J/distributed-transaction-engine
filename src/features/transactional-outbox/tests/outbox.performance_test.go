package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
)

func BenchmarkValidatePartitionKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := contracts.ValidatePartitionKey(schemaSagaID, schemaSagaID); err != nil {
			b.Fatal(err)
		}
	}
}

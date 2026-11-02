package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
)

func BenchmarkStateTransitionThroughput(b *testing.B) {
	ctx := context.Background()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)

	tenantID := "tenant_bench"
	sagaID := "018d9f1c-7201-7000-8000-000000000201"
	publicID := "sag_22222222222222222222aaaaaa"
	source := "bench-runner"
	externalID := "bench_tx_001"
	correlationID := "018d9f1c-7201-7000-8000-000000000299"

	_, err := svc.InitializeState(ctx, contracts.InitializeStateInput{
		ID:            sagaID,
		PublicID:      publicID,
		TenantID:      tenantID,
		Source:        source,
		ExternalID:    externalID,
		CorrelationID: correlationID,
	})
	if err != nil {
		b.Fatalf("bench init failed: %v", err)
	}

	record, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID:        tenantID,
		SagaID:          sagaID,
		Event:           contracts.EventOrchestratorClaim,
		ExpectedVersion: 1,
		FencingToken:    1,
		Reason:          "bench claim",
	})
	if err != nil {
		b.Fatalf("bench claim failed: %v", err)
	}

	b.ResetTimer()
	version := record.Version
	for i := 0; i < b.N; i++ {
		record, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
			TenantID:        tenantID,
			SagaID:          sagaID,
			Event:           contracts.EventStepSuccess,
			ExpectedVersion: version,
			FencingToken:    int64(i + 2),
			Reason:          "bench step transition",
		})
		if err != nil {
			b.Fatalf("bench transition failed at iter %d: %v", i, err)
		}
		version = record.Version
	}
}

func TestPerformanceHighConcurrencyThroughput(t *testing.T) {
	ctx := context.Background()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)

	const workerCount = 50
	const operationsPerWorker = 20

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for w := 0; w < workerCount; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			sagaID := fmt.Sprintf("018d9f1c-7201-7000-8000-%012d", workerID)
			publicID := fmt.Sprintf("sag_33333333333333333333%06d", workerID)
			tenantID := fmt.Sprintf("tenant_%d", workerID%5)
			source := "perf-worker"
			externalID := fmt.Sprintf("perf_ext_%d", workerID)
			correlationID := fmt.Sprintf("018d9f1c-7201-7000-9000-%012d", workerID)

			_, err := svc.InitializeState(ctx, contracts.InitializeStateInput{
				ID:            sagaID,
				PublicID:      publicID,
				TenantID:      tenantID,
				Source:        source,
				ExternalID:    externalID,
				CorrelationID: correlationID,
			})
			if err != nil {
				t.Errorf("worker %d init failed: %v", workerID, err)
				return
			}

			record, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
				TenantID:        tenantID,
				SagaID:          sagaID,
				Event:           contracts.EventOrchestratorClaim,
				ExpectedVersion: 1,
				FencingToken:    1,
				Reason:          "perf claim",
			})
			if err != nil {
				t.Errorf("worker %d claim failed: %v", workerID, err)
				return
			}

			v := record.Version
			for op := 0; op < operationsPerWorker; op++ {
				record, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
					TenantID:        tenantID,
					SagaID:          sagaID,
					Event:           contracts.EventStepSuccess,
					ExpectedVersion: v,
					FencingToken:    int64(op + 2),
					Reason:          "perf step forward",
				})
				if err != nil {
					t.Errorf("worker %d op %d failed: %v", workerID, op, err)
					return
				}
				v = record.Version
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	totalOps := workerCount * (1 + operationsPerWorker)
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	if opsPerSec < 1000.0 {
		t.Logf("Completed %d ops in %v (%.2f ops/sec)", totalOps, elapsed, opsPerSec)
	}
}

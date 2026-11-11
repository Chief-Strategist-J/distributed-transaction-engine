package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	adapters "distributed-transaction-engine/src/features/fencing-tokens/infrastructure/adapters"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func openLeaseDatabase(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil || db.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type claimResult struct {
	holder string
	lease  *contracts.FencingLease
	err    error
}

func raceForLease(repo *adapters.PostgresFencingRepositoryAdapter, tenant string, resource string, claimers int) []claimResult {
	results := make([]claimResult, claimers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := 0; index < claimers; index++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			holder := fmt.Sprintf("worker-%02d", slot)
			<-start
			lease, err := repo.AcquireOrTakeover(context.Background(), tenant, resource, holder, 30000)
			results[slot] = claimResult{holder: holder, lease: lease, err: err}
		}(index)
	}
	close(start)
	wg.Wait()
	return results
}

func winners(results []claimResult) []claimResult {
	var won []claimResult
	for _, result := range results {
		if result.err == nil && result.lease != nil {
			won = append(won, result)
		}
	}
	return won
}

func TestScenario_ConcurrentClaimersOfOneResourceYieldExactlyOneLeaseHolder(t *testing.T) {
	db := openLeaseDatabase(t)
	repo := adapters.NewPostgresFencingRepositoryAdapter(db)
	tenant, resource := "tenant_fence_"+uuid.NewString()[:8], "saga_"+uuid.NewString()[:8]
	won := winners(raceForLease(repo, tenant, resource, 50))
	if len(won) != 1 || won[0].lease.Epoch != 1 {
		t.Fatalf("exactly one claimer may hold the lease at epoch 1, got %d winners", len(won))
	}
}

func expireLease(t *testing.T, db *sql.DB, tenant string, resource string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE fencing_leases SET expires_at = NOW() - INTERVAL '1 second' WHERE tenant_id = $1 AND resource_id = $2`, tenant, resource); err != nil {
		t.Fatal(err)
	}
}

func TestScenario_EveryTakeoverRoundHasOneWinnerAndTheEpochOnlyIncreasesByOne(t *testing.T) {
	db := openLeaseDatabase(t)
	repo := adapters.NewPostgresFencingRepositoryAdapter(db)
	tenant, resource := "tenant_fence_"+uuid.NewString()[:8], "saga_"+uuid.NewString()[:8]
	previous := winners(raceForLease(repo, tenant, resource, 20))
	if len(previous) != 1 {
		t.Fatalf("round 1 expected a single winner, got %d", len(previous))
	}
	for round := 2; round <= 10; round++ {
		expireLease(t, db, tenant, resource)
		current := winners(raceForLease(repo, tenant, resource, 40))
		if len(current) != 1 || current[0].lease.Epoch != int64(round) {
			t.Fatalf("round %d expected one winner at epoch %d, got %d winners", round, round, len(current))
		}
		stale := previous[0]
		if _, err := repo.Renew(context.Background(), tenant, resource, stale.holder, stale.lease.Epoch, 30000); err == nil {
			t.Fatalf("round %d: the previous holder must not be able to renew at its old epoch %d", round, stale.lease.Epoch)
		}
		if err := repo.Release(context.Background(), tenant, resource, stale.holder, stale.lease.Epoch); err == nil {
			t.Fatalf("round %d: the previous holder must not be able to release the new holder's lease", round)
		}
		if _, err := repo.Renew(context.Background(), tenant, resource, current[0].holder, current[0].lease.Epoch, 30000); err != nil {
			t.Fatalf("round %d: the current holder must renew: %v", round, err)
		}
		previous = current
	}
}

func TestScenario_ALiveLeaseCannotBeTakenOverByAnyoneUntilItExpires(t *testing.T) {
	db := openLeaseDatabase(t)
	repo := adapters.NewPostgresFencingRepositoryAdapter(db)
	tenant, resource := "tenant_fence_"+uuid.NewString()[:8], "saga_"+uuid.NewString()[:8]
	holder := winners(raceForLease(repo, tenant, resource, 1))
	if len(holder) != 1 {
		t.Fatal("setup claim failed")
	}
	if challengers := winners(raceForLease(repo, tenant, resource, 30)); len(challengers) != 0 {
		t.Fatalf("a live lease must not be taken over, %d challengers won", len(challengers))
	}
	lease, err := repo.GetLease(context.Background(), tenant, resource)
	if err != nil || lease.Epoch != 1 || lease.HolderID != holder[0].holder {
		t.Fatalf("the original holder and epoch must be untouched: %+v %v", lease, err)
	}
}

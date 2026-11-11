package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/adapters"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func scratchOCCDatabase(t *testing.T) *sql.DB {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "occ_cas_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)"); _ = admin.Close() })
	parsed, _ := url.Parse(base)
	parsed.Path = "/" + name
	db, _ := sql.Open("postgres", parsed.String())
	t.Cleanup(func() { _ = db.Close() })
	files, _ := filepath.Glob("../../../../database/migrations/sql/*.sql")
	sort.Strings(files)
	for _, file := range files {
		base := filepath.Base(file)
		if strings.HasSuffix(base, ".rollback.sql") || base[:4] > "0016" {
			continue
		}
		body, _ := os.ReadFile(file)
		if _, err = db.Exec(string(body)); err != nil {
			t.Fatalf("apply %s: %v", base, err)
		}
	}
	return db
}

func seedSaga(t *testing.T, db *sql.DB, tenant string) uuid.UUID {
	t.Helper()
	sagaID := uuid.New()
	if _, err := db.Exec(`INSERT INTO saga_instance_states (id, public_id, tenant_id, source, external_id, correlation_id, current_state, version, owner_epoch, fencing_token, created_at, updated_at)
		VALUES ($1, $2, $3, 'TEST', 'ext_occ_saga', $4, 'RUNNING', 1, 1, 0, NOW(), NOW())`,
		sagaID, "sag_"+strings.ReplaceAll(sagaID.String(), "-", "")[:26], tenant, uuid.New()); err != nil {
		t.Fatalf("seed saga: %v", err)
	}
	return sagaID
}

func TestScenario_CASOutboxEventsCarryTheNewFencingEpochAndSagaId(t *testing.T) {
	db := scratchOCCDatabase(t)
	tenant := "tenant_occ_fence"
	sagaID := seedSaga(t, db, tenant)
	adapter := adapters.NewPostgresOCCRepositoryAdapter(db)
	result, err := adapter.ExecuteCAS(context.Background(), contracts.CASMutationInput{
		TenantID: tenant, SagaID: sagaID, ExpectedVersion: 1, ExpectedEpoch: 1, TargetState: "COMPLETED", WorkerID: "w1",
		InputID: "input-1", InputType: contracts.InputType("STEP_RESULT"), InputPayload: "{}",
		OutboxEvents: []contracts.OutboxEventPayload{{Topic: "saga.events", Payload: `{"state":"COMPLETED"}`}},
	})
	if err != nil {
		t.Fatalf("CAS failed: %v", err)
	}
	if !result.Succeeded {
		t.Fatalf("the CAS must succeed for a matching version and epoch, got %+v", result)
	}
	var raw []byte
	if err = db.QueryRow(`SELECT headers FROM outbox_events WHERE tenant_id = $1 AND saga_id = $2`, tenant, sagaID).Scan(&raw); err != nil {
		t.Fatalf("outbox row missing: %v", err)
	}
	var headers map[string]string
	_ = json.Unmarshal(raw, &headers)
	if headers["x-saga-id"] != sagaID.String() || headers["x-fencing-token"] == "" {
		t.Fatalf("outbox headers must carry the saga id and fencing epoch, got %v", headers)
	}
	if headers["x-fencing-token"] != itoa(result.CurrentEpoch) {
		t.Fatalf("fencing header %s must equal the epoch returned by the CAS (%d)", headers["x-fencing-token"], result.CurrentEpoch)
	}
}

func itoa(value int64) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

type contentionOutcome struct {
	successes int
	conflicts int
}

func runContention(t *testing.T, adapter *adapters.PostgresOCCRepositoryAdapter, tenant string, sagaID uuid.UUID, workers int, transitionsPerWorker int) contentionOutcome {
	t.Helper()
	var mu sync.Mutex
	outcome := contentionOutcome{}
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for done := 0; done < transitionsPerWorker; {
				state, err := adapter.DiagnoseState(context.Background(), tenant, sagaID)
				if err != nil {
					t.Errorf("diagnose: %v", err)
					return
				}
				result, err := adapter.ExecuteCAS(context.Background(), contracts.CASMutationInput{
					TenantID: tenant, SagaID: sagaID, ExpectedVersion: state.Version, ExpectedEpoch: state.OwnerEpoch, TargetState: "RUNNING",
					WorkerID: "w", InputID: uuid.NewString(), InputType: contracts.InputType("STEP_RESULT"), InputPayload: "{}",
				})
				if err != nil {
					t.Errorf("cas: %v", err)
					return
				}
				mu.Lock()
				if result.Succeeded {
					outcome.successes++
					done++
				} else {
					outcome.conflicts++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return outcome
}

func TestScenario_ConcurrentWorkersNeverLoseOrDuplicateATransition(t *testing.T) {
	for _, workers := range []int{2, 5, 10, 25} {
		t.Run(fmt.Sprintf("%d_workers", workers), func(t *testing.T) {
			db := scratchOCCDatabase(t)
			tenant := "tenant_occ_contention"
			sagaID := seedSaga(t, db, tenant)
			adapter := adapters.NewPostgresOCCRepositoryAdapter(db)
			const perWorker = 20
			outcome := runContention(t, adapter, tenant, sagaID, workers, perWorker)
			state, err := adapter.DiagnoseState(context.Background(), tenant, sagaID)
			if err != nil {
				t.Fatal(err)
			}
			want := workers * perWorker
			if outcome.successes != want || state.Version != int64(1+want) {
				t.Fatalf("every successful CAS must advance the version exactly once: successes=%d want=%d version=%d", outcome.successes, want, state.Version)
			}
			t.Logf("workers=%d transitions=%d conflicts=%d conflicts/transition=%.2f", workers, want, outcome.conflicts, float64(outcome.conflicts)/float64(want))
		})
	}
}

func TestScenario_RacingWritersOnTheSameVersionHaveExactlyOneWinner(t *testing.T) {
	db := scratchOCCDatabase(t)
	tenant := "tenant_occ_race"
	sagaID := seedSaga(t, db, tenant)
	adapter := adapters.NewPostgresOCCRepositoryAdapter(db)
	const racers = 30
	results := make(chan *contracts.CASResult, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for racer := 0; racer < racers; racer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := adapter.ExecuteCAS(context.Background(), contracts.CASMutationInput{
				TenantID: tenant, SagaID: sagaID, ExpectedVersion: 1, ExpectedEpoch: 1, TargetState: "RUNNING", WorkerID: "w",
				InputID: uuid.NewString(), InputType: contracts.InputType("STEP_RESULT"), InputPayload: "{}",
			})
			if err != nil {
				t.Errorf("cas: %v", err)
				return
			}
			results <- result
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	winners := 0
	for result := range results {
		if result.Succeeded {
			winners++
		} else if result.CurrentVersion != 2 {
			t.Fatalf("a loser must be told the current version 2, got %+v", result)
		}
	}
	if winners != 1 {
		t.Fatalf("exactly one racer may win a version, got %d", winners)
	}
}

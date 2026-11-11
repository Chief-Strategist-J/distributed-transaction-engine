package tests

import (
	"context"
	"database/sql"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
	"distributed-transaction-engine/src/features/idempotency-keys/infrastructure/adapters"
	"distributed-transaction-engine/src/features/idempotency-keys/service"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type idempotencyFixture struct {
	db  *sql.DB
	svc *service.IdempotencyService
}

func newIdempotencyFixture(t *testing.T) idempotencyFixture {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "idk_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		_ = admin.Close()
	})
	parsed, _ := url.Parse(base)
	parsed.Path = "/" + name
	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	matches, _ := filepath.Glob("../../../../database/migrations/sql/*create_idempotency_keys_tables.sql")
	if len(matches) != 1 {
		t.Fatalf("expected exactly one idempotency migration, found %v", matches)
	}
	migration, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	svc := service.NewService(service.ServiceDeps{Repo: adapters.NewPostgresIdempotencyRepositoryAdapter(db), Tracer: observability.NewTracer("idempotency-pg-test")})
	return idempotencyFixture{db: db, svc: svc}
}

func ingestInput(key string, payload map[string]interface{}) contracts.IngestIdempotencyInput {
	return contracts.IngestIdempotencyInput{
		ID: uuid.NewString(), PublicID: "idk_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:26], TenantID: "tenant_acme",
		Source: "API_GATEWAY", ExternalID: key, CorrelationID: uuid.NewString(), PrincipalID: "user_1",
		OperationID: "orders.checkout", IdempotencyKey: key, Payload: payload,
	}
}

func (f idempotencyFixture) expireLease(t *testing.T, key string) {
	t.Helper()
	if _, err := f.db.Exec("UPDATE idempotency_keys SET lease_until = NOW() - INTERVAL '5 seconds' WHERE idempotency_key = $1", key); err != nil {
		t.Fatal(err)
	}
}

func statusOf(err error) int {
	if applicationError, ok := err.(*appErrors.ApplicationError); ok {
		return applicationError.StatusCode
	}
	return 0
}

func TestPostgresCrashedWorkersLockIsReacquiredAfterTheLeaseExpires(t *testing.T) {
	f := newIdempotencyFixture(t)
	ctx := context.Background()
	payload := map[string]interface{}{"orderId": "ord_1", "amount": 10.5}

	first, err := f.svc.Ingest(ctx, ingestInput("key-crash", payload))
	if err != nil || first.Outcome != contracts.OutcomeAcquired {
		t.Fatalf("first ingest must acquire: %+v %v", first, err)
	}
	if _, err = f.svc.Ingest(ctx, ingestInput("key-crash", payload)); statusOf(err) != http.StatusConflict {
		t.Fatalf("while the lease is live a retry must conflict, got %v", err)
	}

	f.expireLease(t, "key-crash")
	retaken, err := f.svc.Ingest(ctx, ingestInput("key-crash", payload))
	if err != nil || retaken.Outcome != contracts.OutcomeAcquired {
		t.Fatalf("after the lease expires the key must be re-acquirable, got %+v %v", retaken, err)
	}
	if retaken.Record.RecoveryFencingToken != 1 {
		t.Fatalf("a takeover must bump the recovery fencing token, got %d", retaken.Record.RecoveryFencingToken)
	}
	var live bool
	if err = f.db.QueryRow("SELECT lease_until > NOW() FROM idempotency_keys WHERE idempotency_key = 'key-crash'").Scan(&live); err != nil || !live {
		t.Fatalf("the takeover must install a fresh lease: %v %v", live, err)
	}
	if _, err = f.svc.Ingest(ctx, ingestInput("key-crash", payload)); statusOf(err) != http.StatusConflict {
		t.Fatalf("after the takeover the new lease must protect the key, got %v", err)
	}
}

func TestPostgresConcurrentTakeoversAdmitExactlyOneWorker(t *testing.T) {
	f := newIdempotencyFixture(t)
	ctx := context.Background()
	payload := map[string]interface{}{"orderId": "ord_2"}
	if _, err := f.svc.Ingest(ctx, ingestInput("key-race", payload)); err != nil {
		t.Fatal(err)
	}
	f.expireLease(t, "key-race")

	var acquired, conflicted atomic.Int64
	var group sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := f.svc.Ingest(ctx, ingestInput("key-race", payload))
			switch {
			case err == nil && result.Outcome == contracts.OutcomeAcquired:
				acquired.Add(1)
			case statusOf(err) == http.StatusConflict:
				conflicted.Add(1)
			}
		}()
	}
	group.Wait()
	if acquired.Load() != 1 || conflicted.Load() != 11 {
		t.Fatalf("exactly one worker may take over an expired lease, acquired=%d conflicted=%d", acquired.Load(), conflicted.Load())
	}
	var token int64
	if err := f.db.QueryRow("SELECT recovery_fencing_token FROM idempotency_keys WHERE idempotency_key = 'key-race'").Scan(&token); err != nil || token != 1 {
		t.Fatalf("the fencing token must have been bumped exactly once, got %d %v", token, err)
	}
}

func TestPostgresLockWithoutALeaseTimestampDoesNotDeadlockForever(t *testing.T) {
	f := newIdempotencyFixture(t)
	ctx := context.Background()
	payload := map[string]interface{}{"orderId": "ord_3"}
	if _, err := f.svc.Ingest(ctx, ingestInput("key-null", payload)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec("UPDATE idempotency_keys SET lease_until = NULL, updated_at = NOW() - INTERVAL '10 minutes' WHERE idempotency_key = 'key-null'"); err != nil {
		t.Fatal(err)
	}
	result, err := f.svc.Ingest(ctx, ingestInput("key-null", payload))
	if err != nil || result.Outcome != contracts.OutcomeAcquired {
		t.Fatalf("a stale lock with no lease timestamp must be recoverable, got %+v %v", result, err)
	}
}

func TestPostgresTakeoverRequiresTheSamePayload(t *testing.T) {
	f := newIdempotencyFixture(t)
	ctx := context.Background()
	if _, err := f.svc.Ingest(ctx, ingestInput("key-diff", map[string]interface{}{"orderId": "ord_4"})); err != nil {
		t.Fatal(err)
	}
	f.expireLease(t, "key-diff")
	if _, err := f.svc.Ingest(ctx, ingestInput("key-diff", map[string]interface{}{"orderId": "ord_OTHER"})); statusOf(err) != http.StatusConflict {
		t.Fatalf("a different payload must never take over someone else's key, got %v", err)
	}
}

func TestPostgresCompletedKeysReplayAndNeverReacquire(t *testing.T) {
	f := newIdempotencyFixture(t)
	ctx := context.Background()
	payload := map[string]interface{}{"orderId": "ord_5"}
	first, err := f.svc.Ingest(ctx, ingestInput("key-done", payload))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := contracts.ResponseSnapshot{StatusCode: 201, Headers: map[string]string{"Content-Type": "application/json"}, Body: map[string]interface{}{"ok": true}}
	if err = f.svc.CommitSuccess(ctx, first.Record.TenantID, first.Record.ID.String(), snapshot, "orders/ord_5"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	f.expireLease(t, "key-done")
	replay, err := f.svc.Ingest(ctx, ingestInput("key-done", payload))
	if err != nil || replay.Outcome != contracts.OutcomeReplay {
		t.Fatalf("a completed key must replay even when its old lease timestamp is in the past, got %+v %v", replay, err)
	}
}

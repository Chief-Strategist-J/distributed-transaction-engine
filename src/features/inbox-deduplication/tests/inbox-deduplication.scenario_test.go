package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	adapters "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/inbox-deduplication/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const (
	expiredRows     = 12000
	freshRows       = 100
	heldRows        = 50
	quarantinedRows = 50
	retention       = 30 * 24 * time.Hour
)

type pruneFixture struct {
	db   *sql.DB
	repo *adapters.PostgresInboxDeduplicationRepositoryAdapter
	svc  *service.InboxDeduplicationService
}

func newPruneFixture(t *testing.T) pruneFixture {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "inbox_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
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
	for _, file := range []string{"0007_create_inbox_deduplication_tables.sql", "0028_inbox_prune_index.sql"} {
		body, readErr := os.ReadFile(filepath.Join("../../../../database/migrations/sql", file))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err = db.Exec(string(body)); err != nil {
			t.Fatalf("apply %s: %v", file, err)
		}
	}
	repo := adapters.NewPostgresInboxDeduplicationRepositoryAdapter(db)
	svc := service.NewInboxDeduplicationService(service.InboxDeduplicationServiceDeps{Repo: repo, Tracer: observability.NewTracer("inbox-scenario")})
	return pruneFixture{db: db, repo: repo, svc: svc}
}

const seedStatement = `INSERT INTO inbox_messages (id, public_id, tenant_id, source, external_id, correlation_id, handler_name, handler_generation,
		content_hash, status, attempt_count, producer_event_time, payload, created_at, updated_at)
	SELECT gen_random_uuid(), 'inb_' || substr(md5(random()::text || n::text || $1::text), 1, 26), 'tenant_' || (n % 5), 'TEST',
	       $1::text || '-' || n, gen_random_uuid(), 'handler', 1, repeat('a', 64), $2, 1, $3::timestamptz, '{}'::jsonb, $3::timestamptz, $3::timestamptz
	FROM generate_series(1, $4::int) AS n`

func (f pruneFixture) seed(t *testing.T, tag string, status string, createdAt time.Time, count int) {
	t.Helper()
	if _, err := f.db.Exec(seedStatement, tag, status, createdAt, count); err != nil {
		t.Fatalf("seed %s: %v", tag, err)
	}
}

func (f pruneFixture) seedMixed(t *testing.T) {
	t.Helper()
	old := time.Now().UTC().Add(-100 * 24 * time.Hour)
	f.seed(t, "expired", "APPLIED", old, expiredRows)
	f.seed(t, "fresh", "APPLIED", time.Now().UTC(), freshRows)
	f.seed(t, "held", "HELD", old, heldRows)
	f.seed(t, "quarantined", "QUARANTINED", old, quarantinedRows)
}

func (f pruneFixture) count(t *testing.T, where string) int {
	t.Helper()
	var count int
	if err := f.db.QueryRow("SELECT COUNT(*) FROM inbox_messages WHERE " + where).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestScenario_PruneDeletesInBoundedBatchesAndOnlyExpiredAppliedRows(t *testing.T) {
	f := newPruneFixture(t)
	f.seedMixed(t)
	cutoff := time.Now().UTC().Add(-retention)
	for index, want := range []int{5000, 5000, 2000, 0} {
		deleted, err := f.repo.PruneApplied(context.Background(), cutoff, 5000)
		if err != nil || deleted != want {
			t.Fatalf("batch %d expected %d deleted, got %d err=%v", index+1, want, deleted, err)
		}
	}
	if remaining := f.count(t, "true"); remaining != freshRows+heldRows+quarantinedRows {
		t.Fatalf("only fresh, held and quarantined rows may remain, found %d", remaining)
	}
	if f.count(t, "status = 'APPLIED' AND created_at < now() - interval '30 days'") != 0 || f.count(t, "status = 'HELD'") != heldRows || f.count(t, "status = 'QUARANTINED'") != quarantinedRows {
		t.Fatal("unresolved HELD and QUARANTINED records must never be pruned")
	}
}

func TestScenario_ServiceBoundsWorkPerTickAndResumes(t *testing.T) {
	f := newPruneFixture(t)
	f.seedMixed(t)
	first, err := f.svc.PruneExpired(context.Background(), retention, 5000, 1)
	if err != nil || first != 5000 {
		t.Fatalf("one batch per call must delete exactly one batch, got %d err=%v", first, err)
	}
	rest, err := f.svc.PruneExpired(context.Background(), retention, 5000, 10)
	if err != nil || rest != expiredRows-5000 {
		t.Fatalf("the next call must finish the backlog, got %d err=%v", rest, err)
	}
	if again, _ := f.svc.PruneExpired(context.Background(), retention, 5000, 10); again != 0 {
		t.Fatalf("pruning must be idempotent, deleted %d more", again)
	}
}

func TestScenario_ConcurrentPrunersNeverDoubleDeleteOrBlock(t *testing.T) {
	f := newPruneFixture(t)
	f.seed(t, "expired", "APPLIED", time.Now().UTC().Add(-100*24*time.Hour), 9000)
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deleted, err := f.svc.PruneExpired(context.Background(), retention, 1000, 20)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("pruner failed: %v", err)
			}
			total += deleted
		}()
	}
	wg.Wait()
	if total != 9000 || f.count(t, "true") != 0 {
		t.Fatalf("concurrent pruners must delete each row exactly once: deleted=%d remaining=%d", total, f.count(t, "true"))
	}
}

func TestScenario_PruneRejectsUnsafeArguments(t *testing.T) {
	f := newPruneFixture(t)
	if _, err := f.repo.PruneApplied(context.Background(), time.Now(), 0); err == nil {
		t.Fatal("a zero batch limit must be rejected")
	}
	if _, err := f.svc.PruneExpired(context.Background(), 0, 100, 1); err == nil {
		t.Fatal("a zero retention would delete everything and must be rejected")
	}
	if _, err := f.svc.PruneExpired(context.Background(), -time.Hour, 100, 1); err == nil {
		t.Fatal("a negative retention must be rejected")
	}
}

func explainPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var raw []byte
	if err := db.QueryRow("EXPLAIN (FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
		t.Fatalf("explain: %v", err)
	}
	var decoded any
	_ = json.Unmarshal(raw, &decoded)
	return string(raw)
}

func TestScenario_QueriesUseTheirIndexes(t *testing.T) {
	f := newPruneFixture(t)
	f.seed(t, "bulk", "APPLIED", time.Now().UTC(), 60000)
	f.seed(t, "expired", "APPLIED", time.Now().UTC().Add(-100*24*time.Hour), 500)
	if _, err := f.db.Exec("ANALYZE inbox_messages"); err != nil {
		t.Fatal(err)
	}
	lookup := explainPlan(t, f.db, `SELECT id FROM inbox_messages WHERE tenant_id = $1 AND handler_name = $2 AND handler_generation = $3 AND external_id = $4`, "tenant_1", "handler", 1, "bulk-1")
	if !strings.Contains(lookup, "inbox_messages_dedup_uk") || strings.Contains(lookup, "Seq Scan") {
		t.Fatalf("the dedup lookup must use its unique index: %s", lookup)
	}
	prune := explainPlan(t, f.db, `SELECT tenant_id, id FROM inbox_messages WHERE status = 'APPLIED' AND created_at < $1 ORDER BY created_at ASC LIMIT 5000`, time.Now().UTC().Add(-retention))
	if !strings.Contains(prune, "idx_inbox_messages_expiry") || strings.Contains(prune, "Seq Scan") {
		t.Fatalf("pruning must be an index range scan: %s", prune)
	}
}

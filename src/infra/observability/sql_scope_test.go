package observability_test

import (
	"context"
	"database/sql"
	stderrors "errors"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/utils"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type scopedFixture struct {
	owner  *sql.DB
	scoped *sql.DB
	plain  *sql.DB
}

func connectionURL(base string, database string, user string, password string) string {
	parsed, _ := url.Parse(base)
	parsed.Path = "/" + database
	if user != "" {
		parsed.User = url.UserPassword(user, password)
	}
	return parsed.String()
}

func applyAllMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	files, _ := filepath.Glob("../../../database/migrations/sql/*.sql")
	sort.Strings(files)
	for _, file := range files {
		if strings.HasSuffix(file, ".rollback.sql") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(body)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(file), err)
		}
	}
}

func newScopedFixture(t *testing.T) scopedFixture {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	suffix := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	name, login := "rls_test_"+suffix, "rls_login_"+suffix
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	owner, _ := sql.Open("postgres", connectionURL(base, name, "", ""))
	applyAllMigrations(t, owner)
	if _, err = admin.Exec("CREATE ROLE " + login + " LOGIN PASSWORD 'pw' NOINHERIT IN ROLE dtx_app, dtx_platform"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = owner.Close()
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		_, _ = admin.Exec("DROP ROLE IF EXISTS " + login)
		_ = admin.Close()
	})
	scoped, err := observability.OpenTracedPostgresScoped(connectionURL(base, name, login, "pw"))
	if err != nil {
		t.Fatal(err)
	}
	plain, _ := sql.Open("postgres", connectionURL(base, name, login, "pw"))
	t.Cleanup(func() { _ = scoped.Close(); _ = plain.Close() })
	return scopedFixture{owner: owner, scoped: scoped, plain: plain}
}

func tenantCtx(tenant string) context.Context {
	return utils.WithTenant(context.Background(), tenant)
}

const insertOutbox = `INSERT INTO outbox_events (id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id, aggregate_sequence,
	destination_topic, partition_key, event_type, payload, status)
	VALUES ($1, $2, $3, $4, 'SAGA', $5, 1, 'topic', $6, 'E', '{}'::jsonb, 'PENDING')`

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func asExecer(tx *sql.Tx) execer { return tx }

func insertOutboxRow(ctx context.Context, db execer, tenant string) error {
	id, saga := uuid.New(), uuid.New()
	publicID := "oxe_" + strings.ReplaceAll(id.String(), "-", "")[:26]
	_, err := db.ExecContext(ctx, insertOutbox, id, publicID, tenant, saga, uuid.NewString(), saga.String())
	return err
}

func countOutbox(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_events").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	return count
}

func requireSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pqErr *pq.Error
	if !stderrors.As(err, &pqErr) || string(pqErr.Code) != code {
		t.Fatalf("expected SQLSTATE %s, got %#v", code, err)
	}
}

func TestScopedDB_EveryTenantTableEnforcesRowLevelSecurity(t *testing.T) {
	f := newScopedFixture(t)
	rows, err := f.owner.Query(`SELECT c.relname FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r','p') AND NOT c.relispartition
		AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'tenant_id' AND NOT a.attisdropped)
		AND (NOT (c.relrowsecurity AND c.relforcerowsecurity) OR NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid))`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		_ = rows.Scan(&table)
		t.Errorf("table %s has a tenant_id column but lacks forced RLS or any policy", table)
	}
}

func TestScopedDB_TenantsCannotSeeOrTouchEachOther(t *testing.T) {
	f := newScopedFixture(t)
	if err := insertOutboxRow(tenantCtx("tenant_a"), f.scoped, "tenant_a"); err != nil {
		t.Fatalf("own insert: %v", err)
	}
	if got := countOutbox(t, tenantCtx("tenant_a"), f.scoped); got != 1 {
		t.Fatalf("tenant_a must see its row, got %d", got)
	}
	if got := countOutbox(t, tenantCtx("tenant_b"), f.scoped); got != 0 {
		t.Fatalf("tenant_b must not see tenant_a rows, got %d", got)
	}
	result, err := f.scoped.ExecContext(tenantCtx("tenant_b"), "UPDATE outbox_events SET status = 'PARKED'")
	if affected, _ := result.RowsAffected(); err != nil || affected != 0 {
		t.Fatalf("cross-tenant update must affect nothing: %d %v", affected, err)
	}
	result, _ = f.scoped.ExecContext(tenantCtx("tenant_b"), "DELETE FROM outbox_events")
	if affected, _ := result.RowsAffected(); affected != 0 {
		t.Fatalf("cross-tenant delete must affect nothing: %d", affected)
	}
}

func TestScopedDB_CannotWriteRowsForAnotherTenant(t *testing.T) {
	f := newScopedFixture(t)
	requireSQLState(t, insertOutboxRow(tenantCtx("tenant_b"), f.scoped, "tenant_a"), "42501")
	if _, err := f.scoped.ExecContext(tenantCtx("tenant_a"), "UPDATE outbox_events SET tenant_id = 'tenant_b'"); err != nil {
		t.Fatalf("update of zero visible rows must be harmless: %v", err)
	}
}

func TestScopedDB_FailsClosedWithoutTenantAndGrantsNothingUnscoped(t *testing.T) {
	f := newScopedFixture(t)
	_ = insertOutboxRow(tenantCtx("tenant_a"), f.scoped, "tenant_a")
	if got := countOutbox(t, context.Background(), f.scoped); got != 0 {
		t.Fatalf("a context without tenant must see nothing, got %d", got)
	}
	_, err := f.plain.Exec("SELECT COUNT(*) FROM outbox_events")
	requireSQLState(t, err, "42501")
}

func TestScopedDB_PlatformContextSeesAllTenants(t *testing.T) {
	f := newScopedFixture(t)
	_ = insertOutboxRow(tenantCtx("tenant_a"), f.scoped, "tenant_a")
	_ = insertOutboxRow(tenantCtx("tenant_b"), f.scoped, "tenant_b")
	if got := countOutbox(t, utils.WithPlatform(context.Background()), f.scoped); got != 2 {
		t.Fatalf("platform must see every tenant, got %d", got)
	}
}

func TestScopedDB_SingleConnectionNeverLeaksScopeBetweenTenants(t *testing.T) {
	f := newScopedFixture(t)
	f.scoped.SetMaxOpenConns(1)
	_ = insertOutboxRow(tenantCtx("tenant_a"), f.scoped, "tenant_a")
	for round := 0; round < 3; round++ {
		if countOutbox(t, tenantCtx("tenant_b"), f.scoped) != 0 || countOutbox(t, tenantCtx("tenant_a"), f.scoped) != 1 {
			t.Fatalf("round %d leaked scope across tenants", round)
		}
		if countOutbox(t, utils.WithPlatform(tenantCtx("tenant_b")), f.scoped) != 1 || countOutbox(t, tenantCtx("tenant_b"), f.scoped) != 0 {
			t.Fatalf("round %d leaked platform scope", round)
		}
	}
}

func TestScopedDB_TransactionsAndRollbacksKeepScope(t *testing.T) {
	f := newScopedFixture(t)
	f.scoped.SetMaxOpenConns(1)
	tx, err := f.scoped.BeginTx(tenantCtx("tenant_a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = insertOutboxRow(tenantCtx("tenant_a"), asExecer(tx), "tenant_a"); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	if got := countOutbox(t, tenantCtx("tenant_b"), f.scoped); got != 0 {
		t.Fatalf("after rollback tenant_b must see nothing, got %d", got)
	}
	if got := countOutbox(t, tenantCtx("tenant_a"), f.scoped); got != 0 {
		t.Fatalf("rolled back row must not exist, got %d", got)
	}
}

func TestScopedDB_HostileTenantValuesAreInert(t *testing.T) {
	f := newScopedFixture(t)
	_ = insertOutboxRow(tenantCtx("tenant_a"), f.scoped, "tenant_a")
	for _, hostile := range []string{"' OR '1'='1", "tenant_a'; --", "TENANT_A", " tenant_a"} {
		if got := countOutbox(t, tenantCtx(hostile), f.scoped); got != 0 {
			t.Fatalf("hostile tenant %q saw %d rows", hostile, got)
		}
	}
}

func TestScopedDB_NulByteTenantFailsClosed(t *testing.T) {
	f := newScopedFixture(t)
	_ = insertOutboxRow(tenantCtx("tenant_a"), f.scoped, "tenant_a")
	var count int
	err := f.scoped.QueryRowContext(tenantCtx("tenant_a\x00"), "SELECT COUNT(*) FROM outbox_events").Scan(&count)
	if err == nil {
		t.Fatalf("a NUL byte in the tenant must be refused, saw %d rows", count)
	}
}

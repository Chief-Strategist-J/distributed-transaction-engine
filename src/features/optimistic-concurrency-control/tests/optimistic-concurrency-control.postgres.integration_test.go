package tests

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/adapters"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const inboxMigrationPath = "../../../../database/migrations/sql/0007_create_inbox_deduplication_tables.sql"

func openScratchInbox(t *testing.T) *sql.DB {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "occ_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
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
	migration, err := os.ReadFile(inboxMigrationPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(migration)); err != nil {
		t.Fatalf("apply inbox migration: %v", err)
	}
	return db
}

func TestPostgresInputDedupeUsesTheInboxSchema(t *testing.T) {
	db := openScratchInbox(t)
	adapter := adapters.NewPostgresOCCRepositoryAdapter(db)
	ctx := context.Background()
	saga, otherSaga := uuid.New(), uuid.New()

	applied, err := adapter.IsInputApplied(ctx, "tenant-a", saga, "input-1")
	if err != nil || applied {
		t.Fatalf("a new input must not be applied yet: %v %v", applied, err)
	}
	if err = adapter.MarkInputApplied(ctx, "tenant-a", saga, "input-1"); err != nil {
		t.Fatalf("marking an input applied must succeed against the real inbox schema: %v", err)
	}
	if err = adapter.MarkInputApplied(ctx, "tenant-a", saga, "input-1"); err != nil {
		t.Fatalf("marking the same input twice must be idempotent: %v", err)
	}
	if applied, err = adapter.IsInputApplied(ctx, "tenant-a", saga, "input-1"); err != nil || !applied {
		t.Fatalf("the input must be reported as applied: %v %v", applied, err)
	}
	if applied, _ = adapter.IsInputApplied(ctx, "tenant-b", saga, "input-1"); applied {
		t.Fatal("dedupe must be tenant scoped")
	}
	if applied, _ = adapter.IsInputApplied(ctx, "tenant-a", otherSaga, "input-1"); applied {
		t.Fatal("dedupe must be saga scoped")
	}
	var rows int
	if err = db.QueryRow("SELECT count(*) FROM inbox_messages WHERE tenant_id = 'tenant-a'").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("exactly one inbox row expected, got %d (%v)", rows, err)
	}
}

func TestPostgresInputDedupeHandlesLongInputIdentifiers(t *testing.T) {
	db := openScratchInbox(t)
	adapter := adapters.NewPostgresOCCRepositoryAdapter(db)
	ctx := context.Background()
	saga, long := uuid.New(), strings.Repeat("x", 300)
	if err := adapter.MarkInputApplied(ctx, "tenant-a", saga, long); err != nil {
		t.Fatalf("an over-long input id must be hashed, not rejected: %v", err)
	}
	if applied, err := adapter.IsInputApplied(ctx, "tenant-a", saga, long); err != nil || !applied {
		t.Fatalf("hashed input must be found again: %v %v", applied, err)
	}
}

func TestPostgresInputDedupeFailsLoudlyWhenTheStoreIsBroken(t *testing.T) {
	db := openScratchInbox(t)
	adapter := adapters.NewPostgresOCCRepositoryAdapter(db)
	if _, err := db.Exec("DROP TABLE inbox_messages"); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.IsInputApplied(context.Background(), "tenant-a", uuid.New(), "input-1"); err == nil {
		t.Fatal("a broken dedupe store must surface an error instead of reporting the input as new")
	}
}

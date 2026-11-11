package database

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestRunMigrationsSkipsRollbackFiles(t *testing.T) {
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	defer admin.Close()
	name := "mig_test_" + uuid.NewString()[:8]
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
	parsed, _ := url.Parse(base)
	parsed.Path = "/" + name
	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	dir := t.TempDir()
	files := map[string]string{
		"0001_a.sql":          "CREATE TABLE a (id INT);",
		"0001_a.rollback.sql": "DROP TABLE IF EXISTS a;",
	}
	for file, body := range files {
		if err = os.WriteFile(filepath.Join(dir, file), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err = RunMigrations(context.Background(), db, dir); err != nil {
		t.Fatal(err)
	}
	if err = RunMigrations(context.Background(), db, dir); err != nil {
		t.Fatal(err)
	}
	var tables, recorded int
	_ = db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = 'a'").Scan(&tables)
	_ = db.QueryRow("SELECT count(*) FROM schema_migrations WHERE version LIKE '%rollback%'").Scan(&recorded)
	if tables != 1 || recorded != 0 {
		t.Fatalf("rollback files must never run: tables=%d recordedRollbacks=%d", tables, recorded)
	}
}

func TestRunMigrationsIsSafeUnderConcurrentReplicas(t *testing.T) {
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	defer admin.Close()
	name := "mig_lock_" + uuid.NewString()[:8]
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
	parsed, _ := url.Parse(base)
	parsed.Path = "/" + name
	dir := t.TempDir()
	for file, body := range map[string]string{
		"0001_slow.sql":  "SELECT pg_sleep(0.4); CREATE TABLE slow_one (id INT);",
		"0002_other.sql": "CREATE TABLE slow_two (id INT);",
	} {
		if err = os.WriteFile(filepath.Join(dir, file), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	results := make(chan error, 4)
	for replica := 0; replica < 4; replica++ {
		go func() {
			db, openErr := sql.Open("postgres", parsed.String())
			if openErr != nil {
				results <- openErr
				return
			}
			defer db.Close()
			results <- RunMigrations(context.Background(), db, dir)
		}()
	}
	for replica := 0; replica < 4; replica++ {
		if runErr := <-results; runErr != nil {
			t.Fatalf("concurrent replicas must serialize on the migration lock, got %v", runErr)
		}
	}
}

func scratchMigrationDB(t *testing.T) *sql.DB {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "mig_sum_" + uuid.NewString()[:8]
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
	return db
}

func writeMigration(t *testing.T, dir string, file string, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRunMigrationsRecordsChecksumsAndAcceptsUnchangedFiles(t *testing.T) {
	db := scratchMigrationDB(t)
	dir := t.TempDir()
	writeMigration(t, dir, "0001_a.sql", "CREATE TABLE a (id INT);")
	for run := 0; run < 2; run++ {
		if err := RunMigrations(context.Background(), db, dir); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
	}
	var checksum string
	if err := db.QueryRow("SELECT checksum FROM schema_migrations WHERE version = '0001_a.sql'").Scan(&checksum); err != nil || checksum != FileChecksum([]byte("CREATE TABLE a (id INT);")) {
		t.Fatalf("checksum not recorded correctly: %q %v", checksum, err)
	}
}

func TestRunMigrationsRefusesAnEditedAppliedMigration(t *testing.T) {
	db := scratchMigrationDB(t)
	dir := t.TempDir()
	writeMigration(t, dir, "0001_a.sql", "CREATE TABLE a (id INT);")
	if err := RunMigrations(context.Background(), db, dir); err != nil {
		t.Fatal(err)
	}
	writeMigration(t, dir, "0001_a.sql", "CREATE TABLE a (id INT, extra INT);")
	err := RunMigrations(context.Background(), db, dir)
	if err == nil || !strings.Contains(err.Error(), "modified after it was applied") {
		t.Fatalf("an edited applied migration must be refused, got %v", err)
	}
}

func TestRunMigrationsBackfillsChecksumsForLegacyRows(t *testing.T) {
	db := scratchMigrationDB(t)
	dir := t.TempDir()
	writeMigration(t, dir, "0001_a.sql", "CREATE TABLE a (id INT);")
	legacy := []string{
		"CREATE TABLE schema_migrations (version VARCHAR(255) PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
		"CREATE TABLE a (id INT)",
		"INSERT INTO schema_migrations (version) VALUES ('0001_a.sql')",
	}
	for _, statement := range legacy {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := RunMigrations(context.Background(), db, dir); err != nil {
		t.Fatalf("legacy databases must upgrade in place: %v", err)
	}
	var checksum sql.NullString
	if err := db.QueryRow("SELECT checksum FROM schema_migrations WHERE version = '0001_a.sql'").Scan(&checksum); err != nil || !checksum.Valid {
		t.Fatalf("legacy checksum must be backfilled: %v %v", checksum, err)
	}
}

func TestRunMigrationsFailedFileLeavesNoRecord(t *testing.T) {
	db := scratchMigrationDB(t)
	dir := t.TempDir()
	writeMigration(t, dir, "0001_bad.sql", "CREATE TABLE ok (id INT); THIS IS NOT SQL;")
	if err := RunMigrations(context.Background(), db, dir); err == nil {
		t.Fatal("a failing migration must stop the run")
	}
	var count int
	_ = db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&count)
	var tables int
	_ = db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = 'ok'").Scan(&tables)
	if count != 0 || tables != 0 {
		t.Fatalf("a failed migration must be atomic: records=%d tables=%d", count, tables)
	}
}

func TestSchemaLockMatchesTheCommittedMigrations(t *testing.T) {
	dir, lockPath := "../../../database/migrations/sql", "../../../database/schema.lock"
	if os.Getenv("UPDATE_SCHEMA_LOCK") == "1" {
		lock, err := BuildSchemaLock(dir)
		if err != nil {
			t.Fatal(err)
		}
		rendered, _ := lock.Render()
		if err = os.WriteFile(lockPath, rendered, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := VerifySchemaLock(dir, lockPath); err != nil {
		t.Fatalf("database/schema.lock is out of date (%v): run UPDATE_SCHEMA_LOCK=1 go test ./src/infra/database -run SchemaLock", err)
	}
}

func TestMigrationNumbersAreUniqueExceptTheLegacyDuplicate(t *testing.T) {
	files, err := MigrationFiles("../../../database/migrations/sql")
	if err != nil {
		t.Fatal(err)
	}
	legacyDuplicates := map[string]bool{"0009": true}
	seen := map[string]string{}
	for _, file := range files {
		number := strings.SplitN(file, "_", 2)[0]
		if previous, duplicate := seen[number]; duplicate && !legacyDuplicates[number] {
			t.Fatalf("migration number %s is used by both %s and %s", number, previous, file)
		}
		seen[number] = file
	}
}

func TestSchemaLockDetectsDriftInBothDirections(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "0001_a.sql", "SELECT 1;")
	lock, _ := BuildSchemaLock(dir)
	rendered, _ := lock.Render()
	lockPath := filepath.Join(t.TempDir(), "schema.lock")
	_ = os.WriteFile(lockPath, rendered, 0o600)
	if err := VerifySchemaLock(dir, lockPath); err != nil {
		t.Fatalf("a fresh lock must verify: %v", err)
	}
	writeMigration(t, dir, "0001_a.sql", "SELECT 2;")
	if VerifySchemaLock(dir, lockPath) == nil {
		t.Fatal("an edited migration must be detected")
	}
	writeMigration(t, dir, "0001_a.sql", "SELECT 1;")
	writeMigration(t, dir, "0002_b.sql", "SELECT 3;")
	if VerifySchemaLock(dir, lockPath) == nil {
		t.Fatal("a migration missing from the lock must be detected")
	}
	_ = os.Remove(filepath.Join(dir, "0002_b.sql"))
	_ = os.Remove(filepath.Join(dir, "0001_a.sql"))
	if VerifySchemaLock(dir, lockPath) == nil {
		t.Fatal("a lock entry without a file must be detected")
	}
}

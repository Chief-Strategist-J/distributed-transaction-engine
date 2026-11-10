package database

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
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

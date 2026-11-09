package tests

import (
	"database/sql"
	"os"
	"testing"

	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/adapters"
	_ "github.com/lib/pq"
)

func TestPostgres_Integration(t *testing.T) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@127.0.0.1:5432/distributed_engine?sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil || db.Ping() != nil {
		t.Skipf("PostgreSQL is offline, skipping real DB tests")
		return
	}
	defer db.Close()

	adapter := adapters.NewPostgresCompensationRetryRepositoryAdapter(db)
	if adapter == nil {
		t.Fatalf("expected adapter to be initialized")
	}
}


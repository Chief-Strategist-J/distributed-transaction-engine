package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/machines"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/rules"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/adapters"
	"distributed-transaction-engine/src/features/fencing-tokens/service"
	"distributed-transaction-engine/src/infra/observability"
	_ "github.com/lib/pq"
)

func TestPostgresFencingAdapter_Integration(t *testing.T) {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("DB_PASSWORD")
	if password == "" {
		password = "supersecretpgpassword"
	}
	dbname := os.Getenv("DB_NAME")
	if dbname == "" {
		dbname = "dist_tx_engine"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbname)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to open database handle: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("skipping Postgres integration test: database not reachable at %s:%s (%v)", host, port, err)
	}

	tracer := observability.NewTracer("fencing-postgres-test")
	repo := adapters.NewPostgresFencingRepositoryAdapter(db)
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewFencingRules(),
		Machine: machines.NewFencingLeaseMachine(),
		Tracer:  tracer,
	})

	ctx := context.Background()
	now := time.Now().UTC()
	tenantID := fmt.Sprintf("tenant_fnl_pg_%d", now.UnixNano())
	resourceID := fmt.Sprintf("saga_fnl_pg_%d", now.UnixNano())

	lease, err := svc.AcquireLease(ctx, contracts.AcquireLeaseInput{
		TenantID:        tenantID,
		ResourceID:      resourceID,
		HolderID:        "worker_pg_1",
		LeaseDurationMs: 10000,
	})
	if err != nil {
		t.Fatalf("failed to acquire lease in Postgres: %v", err)
	}
	if lease.Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", lease.Epoch)
	}
}

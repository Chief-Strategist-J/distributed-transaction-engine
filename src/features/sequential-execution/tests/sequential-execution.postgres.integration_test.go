package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	adapters "distributed-transaction-engine/src/features/sequential-execution/infrastructure/adapters"
	_ "github.com/lib/pq"
)

func getRealPostgresDB(t *testing.T) *sql.DB {
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
		t.Fatalf("failed to connect to real Postgres: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping Tier 1 real postgres test: postgres not reachable at %s:%s (%v)", host, port, err)
	}
	return db
}

func TestPostgresSequentialExecutionAdapter_RealDatabaseInvariants(t *testing.T) {
	db := getRealPostgresDB(t)
	defer db.Close()

	ctx := context.Background()
	adapter := adapters.NewPostgresSequentialExecutionRepositoryAdapter(db)

	tenantID := fmt.Sprintf("tenant_test_%d", time.Now().UnixNano())
	workflowDefID := "88888888-8888-4888-8888-888888888888"

	_, err := db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, 'wfd_01HXYZ0123456789ABCDEF0123', $2, 'real-test-workflow', 1, 'TEST', 'ext_999',
			'11111111-1111-4111-8111-111111111111', 'hash_real', '{"steps":[]}', 'ACTIVE', 3, NOW(), NOW()
		) ON CONFLICT DO NOTHING;
	`, workflowDefID, tenantID)
	if err != nil {
		t.Fatalf("failed to insert test workflow definition: %v", err)
	}

	sagaID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	publicID := "seq_01HXYZ0123456789ABCDEFREAL"
	now := time.Now().UTC()

	saga := &contracts.SequentialSagaState{
		ID:                   sagaID,
		PublicID:             publicID,
		TenantID:             tenantID,
		WorkflowDefinitionID: workflowDefID,
		CurrentState:         "RUNNING",
		CurrentStepIndex:     0,
		TotalSteps:           3,
		Phase:                contracts.PhaseForward,
		PivotReached:         false,
		Version:              1,
		OwnerEpoch:           1,
		FinalOutput:          map[string]interface{}{"init": true},
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	if err := adapter.CreateSequentialSaga(ctx, saga); err != nil {
		t.Fatalf("CreateSequentialSaga failed on real Postgres: %v", err)
	}

	duplicateErr := adapter.CreateSequentialSaga(ctx, saga)
	if duplicateErr == nil {
		t.Fatal("expected unique constraint violation on duplicate insert, got nil")
	}

	fetched, err := adapter.GetSequentialSaga(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("GetSequentialSaga failed: %v", err)
	}
	if fetched.CurrentState != "RUNNING" || fetched.Version != 1 || fetched.TotalSteps != 3 {
		t.Fatalf("unexpected fetched state: %+v", fetched)
	}

	err = adapter.UpdateSagaCAS(
		ctx,
		tenantID,
		sagaID,
		1, // expected version
		1, // owner epoch
		"RUNNING",
		1, // next step index
		contracts.PhaseForward,
		false,
		map[string]interface{}{"step0": "completed"},
	)
	if err != nil {
		t.Fatalf("UpdateSagaCAS failed: %v", err)
	}

	staleErr := adapter.UpdateSagaCAS(
		ctx,
		tenantID,
		sagaID,
		1, // STALE version! Real version is now 2
		1,
		"RUNNING",
		2,
		contracts.PhaseForward,
		false,
		map[string]interface{}{"step1": "stale"},
	)
	if staleErr == nil {
		t.Fatal("expected conflict error on stale CAS version, got nil")
	}

	updated, err := adapter.GetSequentialSaga(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("failed to fetch updated saga: %v", err)
	}
	if updated.Version != 2 || updated.CurrentStepIndex != 1 {
		t.Fatalf("expected version 2 step 1, got version %d step %d", updated.Version, updated.CurrentStepIndex)
	}

	ctxEntry := &contracts.StepContextEntry{
		ID:                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		PublicID:          "ctx_01HXYZ0123456789ABCDEFREAL",
		TenantID:          tenantID,
		SagaID:            sagaID,
		ProducerStepIndex: 0,
		EntryName:         "auth_token",
		EntryValue:        map[string]interface{}{"token": "secret_abc"},
		CorrelationID:     "11111111-1111-4111-8111-111111111111",
	}
	if err := adapter.SaveContextEntry(ctx, ctxEntry); err != nil {
		t.Fatalf("SaveContextEntry failed: %v", err)
	}

	entries, err := adapter.GetContextEntries(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("GetContextEntries failed: %v", err)
	}
	if len(entries) != 1 || entries[0].EntryName != "auth_token" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

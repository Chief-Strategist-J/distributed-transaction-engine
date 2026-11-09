package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	adapters "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/adapters"
	"github.com/google/uuid"
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

func TestPostgresOutboxAdapter_RealDatabaseInvariants(t *testing.T) {
	db := getRealPostgresDB(t)
	defer db.Close()

	ctx := context.Background()
	adapter := adapters.NewPostgresOutboxRepositoryAdapter(db)

	tenantID := fmt.Sprintf("tenant_ox_%d", time.Now().UnixNano())
	sagaID := uuid.New().String()
	eventID := uuid.New().String()
	eventPubID := fmt.Sprintf("oxe_%026d", time.Now().Unix()%10000000000)

	_, err := db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, 'wfd_01HXYZ0123456789ABCDEF0123', $2, 'outbox-test-wf', 1, 'TEST', 'ext_ox_999',
			'11111111-1111-4111-8111-111111111111', 'hash_real', '{"steps":[]}', 'ACTIVE', 1, NOW(), NOW()
		) ON CONFLICT DO NOTHING;
	`, sagaID, tenantID)
	if err != nil {
		t.Fatalf("failed to seed workflow definition: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO saga_instances (
			id, public_id, tenant_id, workflow_definition_id, workflow_version,
			spec_hash, source, external_id, input_hash, correlation_id,
			current_state, version, current_step_index, total_steps, execution_phase,
			pivot_reached, owner_epoch, deadline_at, initial_payload, final_output,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $1, 1,
			'hash_placeholder', 'SYSTEM', 'ext_init_ox', 'in_hash', $4,
			'RUNNING', 1, 0, 1, 'FORWARD',
			false, 1, NOW() + INTERVAL '1 day', '{}', '{}',
			NOW(), NOW()
		)
	`, sagaID, fmt.Sprintf("seq_%026d", time.Now().Unix()%10000000000), tenantID, uuid.New().String())
	if err != nil {
		t.Fatalf("failed to seed saga instance: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	event := &contracts.OutboxEventInstance{
		ID:                eventID,
		PublicID:          eventPubID,
		TenantID:          tenantID,
		SagaID:            sagaID,
		AggregateType:     "SAGA",
		AggregateID:       sagaID,
		AggregateSequence: 1,
		DestinationTopic:  "dist-tx.events.v1",
		PartitionKey:      sagaID,
		EventType:         "SAGA_CREATED",
		Payload:           map[string]interface{}{"orderId": "ord_99"},
		Headers:           map[string]interface{}{"tenant": tenantID},
	}

	if err := adapter.InsertOutboxEventTx(ctx, tx, event); err != nil {
		_ = tx.Rollback()
		t.Fatalf("InsertOutboxEventTx failed: %v", err)
	}

	duplicateErr := adapter.InsertOutboxEventTx(ctx, tx, event)
	if duplicateErr == nil {
		_ = tx.Rollback()
		t.Fatal("expected duplicate aggregate sequence error, got nil")
	}
	_ = tx.Rollback()

	txReal, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin real tx: %v", err)
	}
	if err := adapter.InsertOutboxEventTx(ctx, txReal, event); err != nil {
		t.Fatalf("real insert failed: %v", err)
	}
	if err := txReal.Commit(); err != nil {
		t.Fatalf("real commit failed: %v", err)
	}

	claimInput := &contracts.ClaimOutboxEventsInput{
		TenantID:      tenantID,
		Topic:         "dist-tx.events.v1",
		WorkerID:      "worker_ox_1",
		LeaseDuration: 30 * time.Second,
		BatchLimit:    10,
	}

	claimed, err := adapter.ClaimPendingEvents(ctx, claimInput)
	if err != nil {
		t.Fatalf("ClaimPendingEvents failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != eventID || claimed[0].Status != contracts.OutboxStatusInFlight {
		t.Fatalf("unexpected claimed events: %+v", claimed)
	}

	confirmInput := &contracts.ConfirmOutboxPublishInput{
		TenantID: tenantID,
		EventID:  eventID,
		WorkerID: "worker_ox_1",
	}
	if err := adapter.ConfirmPublished(ctx, confirmInput); err != nil {
		t.Fatalf("ConfirmPublished failed: %v", err)
	}

	fetched, err := adapter.GetEvent(ctx, tenantID, eventID)
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if fetched.Status != contracts.OutboxStatusPublished || fetched.PublishedAt == nil {
		t.Fatalf("expected PUBLISHED with non-nil publishedAt, got: %+v", fetched)
	}
}

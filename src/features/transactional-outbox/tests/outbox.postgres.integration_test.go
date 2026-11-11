package tests

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	adapters "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/adapters"
	apperrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"github.com/lib/pq"
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
	eventPubID := "oxe_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:26]

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
	`, sagaID, "seq_"+strings.ReplaceAll(uuid.New().String(), "-", "")[:26], tenantID, uuid.New().String())
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

const (
	partitionKeyConstraint = "outbox_events_partition_key_ck"
	checkViolationCode     = "23514"
	rawOutboxInsert        = `INSERT INTO outbox_events (id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
		aggregate_sequence, destination_topic, partition_key, event_type, payload, status)
		VALUES ($1, $2, 't', $3, 'SAGA', $4, $5, 'topic', $6, 'E', '{}'::jsonb, $7)`
)

type legacyOutbox struct {
	pending   string
	published string
	matching  string
}

func openScratchOutboxDB(t *testing.T) *sql.DB {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "ox_pk_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
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

func applyMigrationFile(t *testing.T, db *sql.DB, file string) {
	t.Helper()
	body, err := os.ReadFile("../../../../database/migrations/sql/" + file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(body)); err != nil {
		t.Fatalf("apply %s: %v", file, err)
	}
}

func insertRawOutbox(db *sql.DB, sagaID string, sequence int, key string, status string) error {
	id := uuid.New()
	publicID := "oxe_" + strings.ReplaceAll(id.String(), "-", "")[:26]
	_, err := db.Exec(rawOutboxInsert, id, publicID, sagaID, uuid.NewString(), sequence, key, status)
	return err
}

func requirePartitionKeyViolation(t *testing.T, err error) {
	t.Helper()
	var pqErr *pq.Error
	if !stderrors.As(err, &pqErr) || string(pqErr.Code) != checkViolationCode || pqErr.Constraint != partitionKeyConstraint {
		t.Fatalf("expected %s on %s, got %#v", checkViolationCode, partitionKeyConstraint, err)
	}
}

func seedLegacyOutbox(t *testing.T, db *sql.DB) legacyOutbox {
	t.Helper()
	legacy := legacyOutbox{pending: uuid.NewString(), published: uuid.NewString(), matching: uuid.NewString()}
	rows := []struct {
		saga, key, status string
		sequence          int
	}{
		{legacy.pending, "order-1", "PENDING", 1},
		{legacy.pending, "order-1", "IN_FLIGHT", 2},
		{legacy.published, "legacy-key", "PUBLISHED", 1},
		{legacy.matching, legacy.matching, "PENDING", 1},
	}
	for _, row := range rows {
		if err := insertRawOutbox(db, row.saga, row.sequence, row.key, row.status); err != nil {
			t.Fatalf("seed legacy row: %v", err)
		}
	}
	return legacy
}

func scanString(t *testing.T, db *sql.DB, query string, args ...interface{}) string {
	t.Helper()
	var value string
	if err := db.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return value
}

func TestPostgresOutbox_PartitionKeyMigrationBackfillsEveryRow(t *testing.T) {
	db := openScratchOutboxDB(t)
	applyMigrationFile(t, db, "0008_create_transactional_outbox_tables.sql")
	legacy := seedLegacyOutbox(t, db)
	applyMigrationFile(t, db, "0025_enforce_outbox_partition_key.sql")
	if got := scanString(t, db, `SELECT COUNT(*)::text FROM outbox_events WHERE partition_key <> saga_id::text`); got != "0" {
		t.Fatalf("every row must be backfilled to saga_id, mismatched=%s", got)
	}
	if got := scanString(t, db, `SELECT partition_key FROM outbox_events WHERE saga_id = $1`, legacy.published); got != legacy.published {
		t.Fatalf("published legacy row must carry its saga id, got %q", got)
	}
	if got := scanString(t, db, `SELECT convalidated::text FROM pg_constraint WHERE conname = $1`, partitionKeyConstraint); got != "true" {
		t.Fatalf("constraint must end validated, got %s", got)
	}
}

func TestPostgresOutbox_PartitionKeyConstraintRejectsInvalidKeys(t *testing.T) {
	db := openScratchOutboxDB(t)
	applyMigrationFile(t, db, "0008_create_transactional_outbox_tables.sql")
	applyMigrationFile(t, db, "0025_enforce_outbox_partition_key.sql")
	saga := uuid.NewString()
	invalid := map[string]string{"empty": "", "blank": "  ", "other_saga": uuid.NewString(), "business_key": "order-9", "uppercase": strings.ToUpper(saga)}
	for name, key := range invalid {
		t.Run(name, func(t *testing.T) { requirePartitionKeyViolation(t, insertRawOutbox(db, saga, 1, key, "PENDING")) })
	}
	if err := insertRawOutbox(db, saga, 1, saga, "PENDING"); err != nil {
		t.Fatalf("saga id key must be accepted: %v", err)
	}
	_, err := db.Exec(`UPDATE outbox_events SET partition_key = 'drift' WHERE saga_id = $1`, saga)
	requirePartitionKeyViolation(t, err)
}

func TestPostgresOutbox_PartitionKeyRollbackRestoresPreviousBehaviour(t *testing.T) {
	db := openScratchOutboxDB(t)
	applyMigrationFile(t, db, "0008_create_transactional_outbox_tables.sql")
	applyMigrationFile(t, db, "0025_enforce_outbox_partition_key.sql")
	applyMigrationFile(t, db, "0025_enforce_outbox_partition_key.rollback.sql")
	if err := insertRawOutbox(db, uuid.NewString(), 1, "anything", "PENDING"); err != nil {
		t.Fatalf("rollback must remove the constraint: %v", err)
	}
}

func TestPostgresOutbox_AdapterRejectsInvalidKeyBeforeDatabase(t *testing.T) {
	db := openScratchOutboxDB(t)
	applyMigrationFile(t, db, "0008_create_transactional_outbox_tables.sql")
	applyMigrationFile(t, db, "0025_enforce_outbox_partition_key.sql")
	adapter := adapters.NewPostgresOutboxRepositoryAdapter(db)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	event := &contracts.OutboxEventInstance{ID: uuid.NewString(), SagaID: uuid.NewString(), PartitionKey: "business-key"}
	var appErr *apperrors.ApplicationError
	if err = adapter.InsertOutboxEventTx(context.Background(), tx, event); !stderrors.As(err, &appErr) || appErr.StatusCode != 400 {
		t.Fatalf("expected 400 validation from adapter, got %#v", err)
	}
}

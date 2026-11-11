package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	adapters "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/inbox-deduplication/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestInboxDeduplicationPostgresIntegration(t *testing.T) {
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
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("skipping test: database not reachable: %v", err)
	}

	tracer := observability.NewTracer("inbox-deduplication-postgres-test")
	repo := adapters.NewPostgresInboxDeduplicationRepositoryAdapter(db)
	svc := service.NewInboxDeduplicationService(service.InboxDeduplicationServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	ctx := context.Background()
	now := time.Now().UTC()
	tenantID := fmt.Sprintf("tenant_inb_pg_%d", now.UnixNano())
	msgID := "01931390-8888-7123-8123-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12]
	pubID := fmt.Sprintf("inb_%026d", now.UnixNano()%1000000000000000000)

	input := &contracts.IngestMessageInput{
		ID:                msgID,
		PublicID:          pubID,
		TenantID:          tenantID,
		Source:            "KAFKA_SETTLEMENT_REPLY",
		ExternalID:        fmt.Sprintf("evt_settle_%d", now.UnixNano()),
		CorrelationID:     msgID,
		HandlerName:       "settlement_handler",
		HandlerGeneration: 1,
		ProducerEventTime: now,
		Payload:           map[string]interface{}{"settlementId": "set_101", "status": "SETTLED"},
	}

	dec1, err := svc.ProcessIncomingMessage(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error on postgres first ingest: %v", err)
	}
	if dec1.Outcome != contracts.OutcomeApplied {
		t.Fatalf("expected OutcomeApplied, got %v", dec1.Outcome)
	}

	dec2, err := svc.ProcessIncomingMessage(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error on postgres duplicate ingest: %v", err)
	}
	if dec2.Outcome != contracts.OutcomeDuplicate {
		t.Fatalf("expected OutcomeDuplicate, got %v", dec2.Outcome)
	}

	record, err := svc.GetRecord(ctx, tenantID, input.HandlerName, input.HandlerGeneration, input.ExternalID)
	if err != nil {
		t.Fatalf("unexpected error reading persisted record: %v", err)
	}
	if record.ContentHash != dec1.StoredHash {
		t.Fatalf("expected stored hash %s, got %s", dec1.StoredHash, record.ContentHash)
	}
}

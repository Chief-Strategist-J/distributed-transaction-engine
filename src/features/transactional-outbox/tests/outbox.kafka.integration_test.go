package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	adapters "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/transactional-outbox/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestTransactionalOutbox_RealKafkaAndPostgresEndToEnd(t *testing.T) {
	db := getRealPostgresDB(t)
	defer db.Close()

	brokerHost := os.Getenv("KAFKA_HOST")
	if brokerHost == "" {
		brokerHost = "127.0.0.1"
	}
	brokerPortStr := os.Getenv("KAFKA_PORT")
	brokerPort := 9092
	if brokerPortStr != "" {
		fmt.Sscanf(brokerPortStr, "%d", &brokerPort)
	}

	testTopic := fmt.Sprintf("test-outbox-%d", time.Now().UnixNano())
	if err := adapters.EnsureKafkaTopic(brokerHost, brokerPort, testTopic); err != nil {
		t.Skipf("skipping Tier 1 real Kafka test: cannot create topic on %s:%d (%v)", brokerHost, brokerPort, err)
	}

	ctx := context.Background()
	tenantID := fmt.Sprintf("tenant_ox_e2e_%d", time.Now().UnixNano())
	sagaID := uuid.New().String()
	eventID := uuid.New().String()
	eventPubID := fmt.Sprintf("oxe_%026d", time.Now().Unix()%10000000000)

	_, err := db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, 'wfd_01HXYZ0123456789ABCDEF0123', $2, 'outbox-kafka-wf', 1, 'TEST', 'ext_ox_k',
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
			'hash_placeholder', 'SYSTEM', 'ext_init_ox_k', 'in_hash', $4,
			'RUNNING', 1, 0, 1, 'FORWARD',
			false, 1, NOW() + INTERVAL '1 day', '{}', '{}',
			NOW(), NOW()
		)
	`, sagaID, fmt.Sprintf("seq_%026d", time.Now().Unix()%10000000000), tenantID, uuid.New().String())
	if err != nil {
		t.Fatalf("failed to seed saga instance: %v", err)
	}

	repo := adapters.NewPostgresOutboxRepositoryAdapter(db)
	publisher := adapters.NewKafkaPublisherAdapter(fmt.Sprintf("%s:%d", brokerHost, brokerPort))
	defer publisher.Close()
	machine := machines.NewOutboxStateMachine()
	tracer := observability.NewTracer("outbox-test")

	outboxSvc := service.NewOutboxService(service.OutboxServiceDeps{
		Repo:      repo,
		Publisher: publisher,
		Machine:   machine,
		Tracer:    tracer,
	})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}

	event := &contracts.OutboxEventInstance{
		ID:                eventID,
		PublicID:          eventPubID,
		TenantID:          tenantID,
		SagaID:            sagaID,
		AggregateType:     "SAGA",
		AggregateID:       sagaID,
		AggregateSequence: 1,
		DestinationTopic:  testTopic,
		PartitionKey:      sagaID,
		EventType:         "ORDER_FULFILLED",
		Payload:           map[string]interface{}{"status": "APPROVED", "amount": 5500},
		Headers:           map[string]interface{}{"x-correlation-id": uuid.New().String()},
	}

	if err := repo.InsertOutboxEventTx(ctx, tx, event); err != nil {
		_ = tx.Rollback()
		t.Fatalf("InsertOutboxEventTx failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	relayedCount, err := outboxSvc.RelayPendingEvents(ctx, tenantID, testTopic, "worker_kafka_1", 10, 30*time.Second)
	if err != nil {
		t.Fatalf("RelayPendingEvents failed: %v", err)
	}

	debugEvent, _ := repo.GetEvent(ctx, tenantID, eventID)
	if debugEvent != nil {
		t.Logf("DebugEvent: Status=%s, LeaseOwner=%s, LastError=%s", debugEvent.Status, debugEvent.LeaseOwner, debugEvent.LastError)
	}

	if relayedCount != 1 {
		t.Fatalf("expected 1 published event, got %d", relayedCount)
	}

	publishedEvent, err := repo.GetEvent(ctx, tenantID, eventID)
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if publishedEvent.Status != contracts.OutboxStatusPublished || publishedEvent.PublishedAt == nil {
		t.Fatalf("expected event to be PUBLISHED, got %+v", publishedEvent)
	}
}

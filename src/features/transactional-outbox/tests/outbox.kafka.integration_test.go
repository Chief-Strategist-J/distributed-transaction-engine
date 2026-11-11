package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	rules "distributed-transaction-engine/src/features/transactional-outbox/domain/rules"
	adapters "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/transactional-outbox/service"
	"distributed-transaction-engine/src/infra/messaging"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
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
	eventPubID := "oxe_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:26]

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
	`, sagaID, "seq_"+strings.ReplaceAll(uuid.New().String(), "-", "")[:26], tenantID, uuid.New().String())
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

func kafkaTestAddress(t *testing.T) string {
	t.Helper()
	host, port := os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "9092"
	}
	address := host + ":" + port
	conn, err := kafka.DialContext(context.Background(), "tcp", address)
	if err != nil {
		t.Skipf("skipping Tier 1 real Kafka test: %v", err)
	}
	_ = conn.Close()
	return address
}

func provisionTestTopic(t *testing.T, address string, partitions int) string {
	t.Helper()
	topic := "test-batch-" + uuid.NewString()
	manifest := messaging.Manifest{Topics: []messaging.TopicSpec{{Name: topic, Partitions: partitions, ReplicationFactor: 1}}}
	report, err := messaging.Reconcile(context.Background(), []string{address}, manifest, true)
	if err != nil || len(report.Created) != 1 {
		t.Fatalf("topic provisioning failed: %+v %v", report, err)
	}
	return topic
}

func readAll(t *testing.T, address string, topic string, partitions int, expected int) []kafka.Message {
	t.Helper()
	var messages []kafka.Message
	for partition := 0; partition < partitions; partition++ {
		reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{address}, Topic: topic, Partition: partition, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		for ctx.Err() == nil && len(messages) < expected {
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				break
			}
			messages = append(messages, message)
		}
		cancel()
		_ = reader.Close()
	}
	return messages
}

func TestKafkaPublisher_BatchKeepsPerKeyOrderAcrossPartitions(t *testing.T) {
	address := kafkaTestAddress(t)
	topic := provisionTestTopic(t, address, 3)
	publisher := adapters.NewKafkaPublisherAdapter(address)
	defer publisher.Close()
	requests := make([]contracts.PublishRequest, 0, 30)
	for index := 0; index < 30; index++ {
		key := fmt.Sprintf("saga-%d", index%3)
		requests = append(requests, contracts.PublishRequest{Topic: topic, PartitionKey: key, Payload: []byte(fmt.Sprintf("%s:%02d", key, index))})
	}
	if elapsed := publishWhenTopicIsReady(t, publisher, requests); elapsed > 3*time.Second {
		t.Fatalf("a batch of 30 must not pay a per-message latency, took %v", elapsed)
	}
	assertPerKeyOrder(t, readAll(t, address, topic, 3, 30))
}

func publishWhenTopicIsReady(t *testing.T, publisher *adapters.KafkaPublisherAdapter, requests []contracts.PublishRequest) time.Duration {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		started := time.Now()
		var failure error
		for _, err := range publisher.PublishBatch(context.Background(), requests) {
			if err != nil {
				failure = err
				break
			}
		}
		if failure == nil {
			return time.Since(started)
		}
		if rules.ClassifyPublishError(failure) != rules.ErrorRetriable || time.Now().After(deadline) {
			t.Fatalf("publish failed: %v", failure)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func assertPerKeyOrder(t *testing.T, messages []kafka.Message) {
	t.Helper()
	if len(messages) != 30 {
		t.Fatalf("expected 30 messages, consumed %d", len(messages))
	}
	partitionOfKey, lastIndex := map[string]int{}, map[string]string{}
	for _, message := range messages {
		key := string(message.Key)
		if known, seen := partitionOfKey[key]; seen && known != message.Partition {
			t.Fatalf("key %s was spread over partitions %d and %d", key, known, message.Partition)
		}
		partitionOfKey[key] = message.Partition
		if string(message.Value) <= lastIndex[key] {
			t.Fatalf("key %s delivered out of order: %s after %s", key, message.Value, lastIndex[key])
		}
		lastIndex[key] = string(message.Value)
	}
}

func TestKafkaPublisher_MissingTopicIsReportedAsRetriable(t *testing.T) {
	address := kafkaTestAddress(t)
	publisher := adapters.NewKafkaPublisherAdapter(address)
	defer publisher.Close()
	outcomes := publisher.PublishBatch(context.Background(), []contracts.PublishRequest{{Topic: "missing-" + uuid.NewString(), PartitionKey: "k", Payload: []byte("{}")}})
	if outcomes[0] == nil {
		t.Fatal("publishing to a topic that does not exist must fail instead of auto-creating it")
	}
	if rules.ClassifyPublishError(outcomes[0]) != rules.ErrorRetriable {
		t.Fatalf("a missing topic must be retried with backoff, got %v", outcomes[0])
	}
}

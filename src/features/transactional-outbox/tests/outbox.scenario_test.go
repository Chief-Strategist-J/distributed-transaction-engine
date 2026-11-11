package tests

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	machines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	rules "distributed-transaction-engine/src/features/transactional-outbox/domain/rules"
	adapters "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/transactional-outbox/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

type scriptedPublisher struct {
	mu       sync.Mutex
	fail     func(request contracts.PublishRequest) error
	batches  [][]contracts.PublishRequest
	received []string
}

func (p *scriptedPublisher) PublishMessage(ctx context.Context, topic string, key string, headers map[string]interface{}, payload []byte) error {
	return p.PublishBatch(ctx, []contracts.PublishRequest{{Topic: topic, PartitionKey: key, Headers: headers, Payload: payload}})[0]
}

func (p *scriptedPublisher) PublishBatch(_ context.Context, requests []contracts.PublishRequest) []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches = append(p.batches, requests)
	outcomes := make([]error, len(requests))
	for index, request := range requests {
		if p.fail != nil {
			outcomes[index] = p.fail(request)
		}
		if outcomes[index] == nil {
			p.received = append(p.received, string(request.Payload))
		}
	}
	return outcomes
}

type relayFixture struct {
	db        *sql.DB
	repo      *adapters.PostgresOutboxRepositoryAdapter
	svc       *service.OutboxService
	publisher *scriptedPublisher
}

func newRelayFixture(t *testing.T) relayFixture {
	t.Helper()
	db := openScratchOutboxDB(t)
	for _, file := range []string{"0008_create_transactional_outbox_tables.sql", "0025_enforce_outbox_partition_key.sql", "0027_outbox_relay_backoff.sql"} {
		applyMigrationFile(t, db, file)
	}
	repo := adapters.NewPostgresOutboxRepositoryAdapter(db)
	publisher := &scriptedPublisher{}
	svc := service.NewOutboxService(service.OutboxServiceDeps{Repo: repo, Publisher: publisher, Machine: machines.NewOutboxStateMachine(), Tracer: observability.NewTracer("relay-test")})
	return relayFixture{db: db, repo: repo, svc: svc, publisher: publisher}
}

func (f relayFixture) seed(t *testing.T, saga string, sequence int, payload string) string {
	t.Helper()
	id := uuid.New()
	publicID := "oxe_" + strings.ReplaceAll(id.String(), "-", "")[:26]
	_, err := f.db.Exec(`INSERT INTO outbox_events (id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id, aggregate_sequence,
		destination_topic, partition_key, event_type, payload, status)
		VALUES ($1, $2, 'tenant_r', $3, 'SAGA', $6, $4, 'topic.r', $6, 'E', jsonb_build_object('n', $5::text), 'PENDING')`, id, publicID, saga, sequence, payload, saga)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return id.String()
}

func (f relayFixture) relay(t *testing.T) int {
	t.Helper()
	count, err := f.svc.RelayPendingEvents(context.Background(), "tenant_r", "topic.r", "worker-1", 100, 30*time.Second)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	return count
}

func (f relayFixture) row(t *testing.T, id string) (status string, attempts int, nextAttempt sql.NullTime, lastError sql.NullString) {
	t.Helper()
	err := f.db.QueryRow(`SELECT status, attempt_count, next_attempt_at, last_error FROM outbox_events WHERE id = $1`, id).Scan(&status, &attempts, &nextAttempt, &lastError)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func (f relayFixture) clearBackoff(t *testing.T) {
	t.Helper()
	if _, err := f.db.Exec(`UPDATE outbox_events SET next_attempt_at = NULL`); err != nil {
		t.Fatal(err)
	}
}

func TestScenario_PartialBatchFailureBacksOffOnlyTheFailedEvent(t *testing.T) {
	f := newRelayFixture(t)
	ids := []string{f.seed(t, uuid.NewString(), 1, "a"), f.seed(t, uuid.NewString(), 1, "b"), f.seed(t, uuid.NewString(), 1, "c")}
	f.publisher.fail = func(r contracts.PublishRequest) error {
		if strings.Contains(string(r.Payload), `"b"`) {
			return stderrors.New("kafka: connection reset")
		}
		return nil
	}
	if published := f.relay(t); published != 2 {
		t.Fatalf("expected 2 published, got %d", published)
	}
	status, attempts, next, lastError := f.row(t, ids[1])
	if status != "PENDING" || attempts != 1 || !next.Valid || !strings.Contains(lastError.String, "connection reset") {
		t.Fatalf("failed event must return to PENDING with backoff: %s %d %v %v", status, attempts, next, lastError)
	}
	if wait := time.Until(next.Time); wait < 900*time.Millisecond || wait > 2100*time.Millisecond {
		t.Fatalf("first retry must back off 1-2s, got %v", wait)
	}
	batches := len(f.publisher.batches)
	if published := f.relay(t); published != 0 || len(f.publisher.batches) != batches {
		t.Fatalf("a backing-off event must not be retried immediately (published=%d)", published)
	}
}

func TestScenario_BackedOffEventIsNotRetriedUntilDue(t *testing.T) {
	f := newRelayFixture(t)
	id := f.seed(t, uuid.NewString(), 1, "x")
	f.publisher.fail = func(contracts.PublishRequest) error { return stderrors.New("kafka: timeout") }
	f.relay(t)
	callsBefore := len(f.publisher.batches)
	f.relay(t)
	if len(f.publisher.batches) != callsBefore {
		t.Fatal("publisher must not be called while the event is backing off")
	}
	f.clearBackoff(t)
	f.publisher.fail = nil
	if f.relay(t) != 1 {
		t.Fatal("event must be published once its backoff elapses")
	}
	if status, _, _, _ := f.row(t, id); status != "PUBLISHED" {
		t.Fatalf("expected PUBLISHED, got %s", status)
	}
}

func TestScenario_LaterEventOfAggregateNeverOvertakesAFailingEarlierOne(t *testing.T) {
	f := newRelayFixture(t)
	saga := uuid.NewString()
	first, second := f.seed(t, saga, 1, "first"), f.seed(t, saga, 2, "second")
	f.publisher.fail = func(r contracts.PublishRequest) error { return stderrors.New("kafka: broker unavailable") }
	for round := 0; round < 3; round++ {
		f.relay(t)
		f.clearBackoff(t)
	}
	for _, request := range flatten(f.publisher.batches) {
		if strings.Contains(string(request.Payload), "second") {
			t.Fatal("seq 2 must never be offered while seq 1 is unpublished")
		}
	}
	f.publisher.fail = nil
	f.relay(t)
	f.relay(t)
	if got := strings.Join(f.publisher.received, ","); got != `{"n":"first"},{"n":"second"}` {
		t.Fatalf("expected strict per-aggregate order, got %s", got)
	}
	if status, _, _, _ := f.row(t, first); status != "PUBLISHED" {
		t.Fatalf("first not published: %s", status)
	}
	if status, _, _, _ := f.row(t, second); status != "PUBLISHED" {
		t.Fatalf("second not published: %s", status)
	}
}

func flatten(batches [][]contracts.PublishRequest) []contracts.PublishRequest {
	var all []contracts.PublishRequest
	for _, batch := range batches {
		all = append(all, batch...)
	}
	return all
}

func TestScenario_PermanentTransientFailureParksOnlyAfterTheAttemptLimit(t *testing.T) {
	f := newRelayFixture(t)
	id := f.seed(t, uuid.NewString(), 1, "doomed")
	f.publisher.fail = func(contracts.PublishRequest) error { return stderrors.New("kafka: Unknown Topic Or Partition") }
	for attempt := 1; attempt < rules.MaxPublishAttempts; attempt++ {
		f.relay(t)
		if status, attempts, _, _ := f.row(t, id); status != "PENDING" || attempts != attempt {
			t.Fatalf("attempt %d: expected PENDING with %d attempts, got %s/%d", attempt, attempt, status, attempts)
		}
		f.clearBackoff(t)
	}
	f.relay(t)
	status, attempts, _, lastError := f.row(t, id)
	if status != "PARKED" || attempts != rules.MaxPublishAttempts || !strings.Contains(lastError.String, fmt.Sprintf("exceeded %d publish attempts", rules.MaxPublishAttempts)) {
		t.Fatalf("expected PARKED after %d attempts, got %s/%d/%q", rules.MaxPublishAttempts, status, attempts, lastError.String)
	}
}

func TestScenario_TopicProvisionedLateIsDeliveredNotParked(t *testing.T) {
	f := newRelayFixture(t)
	id := f.seed(t, uuid.NewString(), 1, "late-topic")
	f.publisher.fail = func(contracts.PublishRequest) error { return stderrors.New("kafka: Unknown Topic Or Partition") }
	for round := 0; round < 3; round++ {
		f.relay(t)
		f.clearBackoff(t)
	}
	f.publisher.fail = nil
	f.relay(t)
	if status, attempts, _, _ := f.row(t, id); status != "PUBLISHED" || attempts != 4 {
		t.Fatalf("expected PUBLISHED on the 4th attempt, got %s/%d", status, attempts)
	}
}

func TestScenario_NonRetriableErrorParksImmediately(t *testing.T) {
	f := newRelayFixture(t)
	id := f.seed(t, uuid.NewString(), 1, "huge")
	f.publisher.fail = func(contracts.PublishRequest) error { return stderrors.New("kafka: record too large") }
	f.relay(t)
	if status, attempts, _, _ := f.row(t, id); status != "PARKED" || attempts != 1 {
		t.Fatalf("expected immediate PARKED, got %s/%d", status, attempts)
	}
}

func TestScenario_CrashedWorkersExpiredLeaseIsReclaimed(t *testing.T) {
	f := newRelayFixture(t)
	id := f.seed(t, uuid.NewString(), 1, "orphan")
	claimed, err := f.repo.ClaimPendingEvents(context.Background(), &contracts.ClaimOutboxEventsInput{TenantID: "tenant_r", Topic: "topic.r", WorkerID: "crashed", LeaseDuration: time.Hour, BatchLimit: 10})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("setup claim failed: %v %d", err, len(claimed))
	}
	if f.relay(t) != 0 {
		t.Fatal("a live lease must not be stolen")
	}
	if _, err = f.db.Exec(`UPDATE outbox_events SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if f.relay(t) != 1 {
		t.Fatal("an expired lease must be reclaimed and published")
	}
	if status, _, _, _ := f.row(t, id); status != "PUBLISHED" {
		t.Fatalf("expected PUBLISHED, got %s", status)
	}
}

func TestScenario_WholeBatchIsPublishedInOneCall(t *testing.T) {
	f := newRelayFixture(t)
	for index := 0; index < 50; index++ {
		f.seed(t, uuid.NewString(), 1, fmt.Sprintf("m%d", index))
	}
	if published := f.relay(t); published != 50 {
		t.Fatalf("expected 50 published, got %d", published)
	}
	if len(f.publisher.batches) != 1 || len(f.publisher.batches[0]) != 50 {
		t.Fatalf("expected a single batch of 50, got %d batches", len(f.publisher.batches))
	}
}

func TestScenario_RelayTargetsListOnlyEligibleWork(t *testing.T) {
	f := newRelayFixture(t)
	published := f.seed(t, uuid.NewString(), 1, "done")
	f.seed(t, uuid.NewString(), 1, "ready")
	backingOff := f.seed(t, uuid.NewString(), 1, "later")
	if _, err := f.db.Exec(`UPDATE outbox_events SET status='PUBLISHED', published_at=NOW() WHERE id=$1`, published); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec(`UPDATE outbox_events SET next_attempt_at = NOW() + INTERVAL '1 hour' WHERE id=$1`, backingOff); err != nil {
		t.Fatal(err)
	}
	targets, err := f.repo.ListRelayTargets(context.Background(), 10)
	if err != nil || len(targets) != 1 || targets[0].TenantID != "tenant_r" || targets[0].Topic != "topic.r" {
		t.Fatalf("expected exactly one eligible target, got %v err=%v", targets, err)
	}
}

func TestScenario_RelayAllPendingDrainsEveryTenantAndTopic(t *testing.T) {
	f := newRelayFixture(t)
	f.seed(t, uuid.NewString(), 1, "one")
	other, otherSaga := uuid.New(), uuid.New()
	if _, err := f.db.Exec(`INSERT INTO outbox_events (id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id, aggregate_sequence,
		destination_topic, partition_key, event_type, payload, status)
		VALUES ($1, $2, 'tenant_other', $3, 'SAGA', $4, 1, 'topic.other', $4, 'E', '{}'::jsonb, 'PENDING')`,
		other, "oxe_"+strings.ReplaceAll(other.String(), "-", "")[:26], otherSaga, otherSaga.String()); err != nil {
		t.Fatal(err)
	}
	total, err := f.svc.RelayAllPending(context.Background(), "relay", 100, 30*time.Second)
	if err != nil || total != 2 {
		t.Fatalf("expected 2 published across tenants/topics, got %d err=%v", total, err)
	}
}

const (
	throughputEvents      = 5000
	throughputFloorPerSec = 1000.0
)

func seedThroughputEvents(t *testing.T, db *sql.DB, topic string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO outbox_events (id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id, aggregate_sequence,
		destination_topic, partition_key, event_type, payload, status)
		SELECT gen_random_uuid(), 'oxe_' || substr(replace(gen_random_uuid()::text, '-', ''), 1, 26), 'tenant_r', s.saga, 'SAGA', s.saga::text, 1,
		       $1, s.saga::text, 'E', jsonb_build_object('n', s.i), 'PENDING'
		FROM (SELECT i, gen_random_uuid() AS saga FROM generate_series(1, $2::int) AS i) s`, topic, throughputEvents)
	if err != nil {
		t.Fatalf("seed throughput events: %v", err)
	}
}

func TestScenario_RelayThroughputMeetsTheSLAAgainstRealKafka(t *testing.T) {
	address := kafkaTestAddress(t)
	f := newRelayFixture(t)
	topic := provisionTestTopic(t, address, 6)
	publisher := adapters.NewKafkaPublisherAdapter(address)
	defer publisher.Close()
	publishWhenTopicIsReady(t, publisher, []contracts.PublishRequest{{Topic: topic, PartitionKey: "warm-up", Payload: []byte("{}")}})
	svc := service.NewOutboxService(service.OutboxServiceDeps{Repo: f.repo, Publisher: publisher, Machine: machines.NewOutboxStateMachine(), Tracer: observability.NewTracer("throughput-test")})
	seedThroughputEvents(t, f.db, topic)

	started := time.Now()
	published := 0
	for published < throughputEvents && time.Since(started) < 90*time.Second {
		count, err := svc.RelayPendingEvents(context.Background(), "tenant_r", topic, "relay-perf", 500, 30*time.Second)
		if err != nil {
			t.Fatalf("relay: %v", err)
		}
		published += count
	}
	perSecond := float64(published) / time.Since(started).Seconds()
	t.Logf("relayed %d events at %.0f events/s", published, perSecond)
	if published != throughputEvents || perSecond < throughputFloorPerSec {
		t.Fatalf("relay SLA violated: %d/%d events at %.0f/s (floor %.0f/s)", published, throughputEvents, perSecond, throughputFloorPerSec)
	}
	var unpublished int
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE status <> 'PUBLISHED'`).Scan(&unpublished); err != nil || unpublished != 0 {
		t.Fatalf("every event must be PUBLISHED, %d are not (err=%v)", unpublished, err)
	}
}

package tests

import (
	"context"
	"database/sql"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/machines"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/rules"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/adapters"
	"distributed-transaction-engine/src/features/dead-letter-queue/service"
	appErrors "distributed-transaction-engine/src/shared/errors"
	stderrors "errors"
	"github.com/google/uuid"
)

const replayTenant = "tenant-replay"

type replayFixture struct {
	svc    *service.DeadLetterService
	ledger *adapters.MemoryIdempotencyLedgerAdapter
	record *contracts.DeadLetterRecord
	key    string
}

func newReplayFixture(t *testing.T) replayFixture {
	t.Helper()
	ledger := adapters.NewMemoryIdempotencyLedgerAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo: adapters.NewMemoryDeadLetterRepositoryAdapter(), Rules: rules.NewDeadLetterRules(), Machine: machines.NewDeadLetterMachine(), Ledger: ledger,
	})
	key := "idem-" + uuid.NewString()
	record, err := svc.AdmitRecord(context.Background(), contracts.AdmitDLQInput{
		TenantID: replayTenant, CorrelationID: uuid.New(), SagaID: uuid.New(), StepIndex: 1, StepType: "PAYMENT_STEP", AttemptID: uuid.New(),
		IdempotencyKey: key, OutcomeClass: contracts.OutcomeNotExecuted, FailureClass: contracts.FailureRetryExhausted,
		ErrorFingerprint: "FP", RawMessage: "boom", PayloadRef: "s3://p/1.json", SchemaVersion: 1, Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.TriageRecord(context.Background(), contracts.TriageInput{TenantID: replayTenant, RecordID: record.ID, ActorID: "op"}); err != nil {
		t.Fatal(err)
	}
	return replayFixture{svc: svc, ledger: ledger, record: record, key: key}
}

func (f replayFixture) retry(input contracts.RetryActionInput) error {
	input.TenantID, input.RecordID = replayTenant, f.record.ID
	if input.ActorID == "" {
		input.ActorID = "operator-1"
	}
	_, err := f.svc.RetryRecord(context.Background(), input, "RUNNING", 1)
	return err
}

func requireAppStatus(t *testing.T, err error, status int) {
	t.Helper()
	var appErr *appErrors.ApplicationError
	if !stderrors.As(err, &appErr) || appErr.StatusCode != status {
		t.Fatalf("expected status %d, got %#v", status, err)
	}
}

func TestRules_ReplayIdempotencyVerdictByLedgerState(t *testing.T) {
	r := rules.NewDeadLetterRules()
	cases := []struct {
		state   string
		found   bool
		blocked bool
	}{
		{"", false, false}, {"REJECTED_FINAL", true, false}, {"COMPLETED", true, true},
		{"IN_PROGRESS", true, true}, {"UNKNOWN", true, true}, {"SURPRISE", true, true},
	}
	for _, tc := range cases {
		if got := r.EvaluateReplayIdempotency(tc.state, tc.found); got.Blocked != tc.blocked || got.Reason == "" {
			t.Fatalf("state %q found=%v expected blocked=%v got %+v", tc.state, tc.found, tc.blocked, got)
		}
	}
}

func TestScenario_ReplayIsBlockedWhenTheOriginalExecutionAlreadyCompleted(t *testing.T) {
	for _, state := range []string{"COMPLETED", "IN_PROGRESS", "UNKNOWN"} {
		t.Run(state, func(t *testing.T) {
			f := newReplayFixture(t)
			f.ledger.Record(replayTenant, f.key, state)
			requireAppStatus(t, f.retry(contracts.RetryActionInput{}), http.StatusConflict)
		})
	}
}

func TestScenario_ReplayProceedsWhenNothingWasExecuted(t *testing.T) {
	for _, state := range []string{"", "REJECTED_FINAL"} {
		f := newReplayFixture(t)
		if state != "" {
			f.ledger.Record(replayTenant, f.key, state)
		}
		if err := f.retry(contracts.RetryActionInput{}); err != nil {
			t.Fatalf("ledger state %q must allow replay: %v", state, err)
		}
	}
}

func TestScenario_OverrideNeedsAnExactConfirmationAndAReason(t *testing.T) {
	f := newReplayFixture(t)
	f.ledger.Record(replayTenant, f.key, "COMPLETED")
	cases := map[string]struct {
		input  contracts.RetryActionInput
		status int
	}{
		"override_without_confirmation": {contracts.RetryActionInput{OverrideIdempotency: true, OverrideReason: "customer was never charged"}, http.StatusBadRequest},
		"override_without_reason":       {contracts.RetryActionInput{OverrideIdempotency: true, OverrideConfirmation: contracts.OverrideConfirmationFor(f.key)}, http.StatusBadRequest},
		"short_reason":                  {contracts.RetryActionInput{OverrideIdempotency: true, OverrideConfirmation: contracts.OverrideConfirmationFor(f.key), OverrideReason: "ok"}, http.StatusBadRequest},
		"wrong_key_in_confirmation":     {contracts.RetryActionInput{OverrideIdempotency: true, OverrideConfirmation: contracts.OverrideConfirmationFor("other-key"), OverrideReason: "customer was never charged"}, http.StatusForbidden},
		"confirmation_without_flag":     {contracts.RetryActionInput{OverrideConfirmation: contracts.OverrideConfirmationFor(f.key), OverrideReason: "customer was never charged"}, http.StatusBadRequest},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			requireAppStatus(t, f.retry(tc.input), tc.status)
		})
	}
	if err := f.retry(contracts.RetryActionInput{OverrideIdempotency: true, OverrideConfirmation: contracts.OverrideConfirmationFor(f.key), OverrideReason: "customer was never charged, confirmed with the bank"}); err != nil {
		t.Fatalf("a correctly confirmed override must be accepted: %v", err)
	}
}

func TestScenario_ServiceWithoutALedgerKeepsTheOriginalBehaviour(t *testing.T) {
	f := newReplayFixture(t)
	bare := service.NewService(service.ServiceDeps{Repo: adapters.NewMemoryDeadLetterRepositoryAdapter()})
	if bare == nil {
		t.Fatal("service must construct without a ledger")
	}
	f.ledger.Record(replayTenant, f.key, "COMPLETED")
	if state, found, _ := f.ledger.FindExecutionState(context.Background(), replayTenant, f.key); !found || state != "COMPLETED" {
		t.Fatalf("ledger round trip failed: %q %v", state, found)
	}
}

func TestScenario_PostgresLedgerReadsTheRealIdempotencyTable(t *testing.T) {
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	name := "dlq_led_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)"); _ = admin.Close() })
	parsed, _ := url.Parse(base)
	parsed.Path = "/" + name
	db, _ := sql.Open("postgres", parsed.String())
	t.Cleanup(func() { _ = db.Close() })
	migration, _ := os.ReadFile(filepath.Join("../../../../database/migrations/sql", "0009_create_idempotency_keys_tables.sql"))
	if _, err = db.Exec(string(migration)); err != nil {
		t.Fatalf("apply idempotency migration: %v", err)
	}
	ledger := adapters.NewPostgresIdempotencyLedgerAdapter(db)
	if _, found, lookupErr := ledger.FindExecutionState(context.Background(), "tenant_x", "never-seen"); lookupErr != nil || found {
		t.Fatalf("an unknown key must report not found: %v %v", found, lookupErr)
	}
	for _, state := range []string{"COMPLETED", "IN_PROGRESS", "UNKNOWN", "REJECTED_FINAL"} {
		key := "key-" + state
		id := uuid.New()
		if _, err = db.Exec(`INSERT INTO idempotency_keys (id, public_id, tenant_id, external_id, correlation_id, principal_id, operation_id,
			idempotency_key, request_fingerprint, current_state, expires_at)
			VALUES ($1, $2, 'tenant_x', $3, $4, 'p', 'op', $5, repeat('a', 64), $6, NOW() + INTERVAL '1 day')`,
			id, "idk_"+strings.ReplaceAll(id.String(), "-", "")[:26], key, uuid.New(), key, state); err != nil {
			t.Fatalf("seed %s: %v", state, err)
		}
		got, found, lookupErr := ledger.FindExecutionState(context.Background(), "tenant_x", key)
		if lookupErr != nil || !found || got != state {
			t.Fatalf("expected %s, got %q found=%v err=%v", state, got, found, lookupErr)
		}
		if _, other, _ := ledger.FindExecutionState(context.Background(), "tenant_y", key); other {
			t.Fatalf("the ledger must be tenant scoped, tenant_y saw %s", key)
		}
	}
}

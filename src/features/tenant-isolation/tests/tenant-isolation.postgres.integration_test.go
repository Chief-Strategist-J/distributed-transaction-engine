package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	tenantisolation "distributed-transaction-engine/src/features/tenant-isolation"
	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/machines"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/adapters"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

const migrationRelativePath = "../../../../database/migrations/sql/0022_create_tenant_isolation_tables.sql"
const rollbackRelativePath = "../../../../database/migrations/sql/0022_create_tenant_isolation_tables.rollback.sql"

type postgresFixture struct {
	owner    *sql.DB
	app      *sql.DB
	platform *sql.DB
	appSvc   tenantisolation.Services
	platSvc  tenantisolation.Services
}

func scratchURL(base *url.URL, database string, role string) string {
	scratch := *base
	scratch.Path = "/" + database
	query := scratch.Query()
	if role != "" {
		query.Set("role", role)
	}
	scratch.RawQuery = query.Encode()
	return scratch.String()
}

func openPool(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	pool, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func createScratchDatabase(t *testing.T) (*url.URL, string) {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set, skipping real PostgreSQL tests")
	}
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline, skipping real PostgreSQL tests")
	}
	name := "ti_test_" + uuid.NewString()[:8]
	if _, err = admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatalf("create scratch database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		_ = admin.Close()
	})
	return parsed, name
}

func applyFile(t *testing.T, owner *sql.DB, path string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if _, err = owner.Exec(string(content)); err != nil {
		t.Fatalf("apply %s: %v", path, err)
	}
}

func servicesFor(db *sql.DB, name string) tenantisolation.Services {
	adapter := tenantisolation.NewPostgresAdapter(db)
	runtime := tenantisolation.NewSystemRuntimeAdapter()
	return tenantisolation.NewServices(tenantisolation.ServicesDeps{
		Registry: adapter, Quota: adapter, Quarantine: adapter, Security: adapter, Clock: runtime, IDs: runtime,
		Tracer: observability.NewTracer(name),
	})
}

func newPostgresFixture(t *testing.T) postgresFixture {
	t.Helper()
	base, name := createScratchDatabase(t)
	owner := openPool(t, scratchURL(base, name, ""))
	applyFile(t, owner, migrationRelativePath)
	app := openPool(t, scratchURL(base, name, "dtx_app"))
	platform := openPool(t, scratchURL(base, name, "dtx_platform"))
	return postgresFixture{owner: owner, app: app, platform: platform, appSvc: servicesFor(app, "ti-app"), platSvc: servicesFor(platform, "ti-platform")}
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return count
}

func constraintOf(err error) string {
	var pgErr *pq.Error
	if errors.As(err, &pgErr) {
		return pgErr.Constraint
	}
	return ""
}

func sqlStateOf(err error) string {
	var pgErr *pq.Error
	if errors.As(err, &pgErr) {
		return string(pgErr.Code)
	}
	return ""
}

func platformCreate(t *testing.T, fixture postgresFixture, tenantID string, maxSagas int) contracts.TenantRecord {
	t.Helper()
	record, err := fixture.platSvc.Registry.CreateTenant(context.Background(), contracts.CreateTenantInput{
		TenantID: tenantID, MaxConcurrentSagas: maxSagas, CorrelationID: uuid.New(),
	})
	if err != nil {
		t.Fatalf("platform create %s: %v", tenantID, err)
	}
	return record
}

func TestPostgresRoleHardeningAndForcedRLS(t *testing.T) {
	fixture := newPostgresFixture(t)
	for _, role := range []string{"dtx_app", "dtx_platform"} {
		if countRows(t, fixture.owner, "SELECT count(*) FROM pg_roles WHERE rolname = $1 AND NOT rolsuper AND NOT rolbypassrls AND NOT rolcanlogin", role) != 1 {
			t.Fatalf("role %s must be NOLOGIN, NOSUPERUSER and NOBYPASSRLS", role)
		}
		if countRows(t, fixture.owner, "SELECT count(*) FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner WHERE r.rolname = $1 AND c.relkind = 'r' AND c.relname LIKE 'tenant_%'", role) != 0 {
			t.Fatalf("role %s must not own tenant tables", role)
		}
	}
	forced := countRows(t, fixture.owner, "SELECT count(*) FROM pg_class WHERE relname IN ('tenant_registry','tenant_tombstone','tenant_saga_quota','tenant_message_quarantine','tenant_security_event') AND relrowsecurity AND relforcerowsecurity")
	if forced != 5 {
		t.Fatalf("expected 5 tables with forced RLS, got %d", forced)
	}
}

func TestPostgresMachineMatchesTransitionTable(t *testing.T) {
	fixture := newPostgresFixture(t)
	rows, err := fixture.owner.Query("SELECT from_status, to_status FROM tenant_status_transition ORDER BY from_status, to_status")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	stored := make([]string, 0)
	for rows.Next() {
		var from, to string
		if err = rows.Scan(&from, &to); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, from+">"+to)
	}
	coded := make([]string, 0)
	for _, transition := range machines.ListTenantStatusTransitions() {
		coded = append(coded, string(transition.From)+">"+string(transition.To))
	}
	sort.Strings(stored)
	sort.Strings(coded)
	if fmt.Sprint(stored) != fmt.Sprint(coded) {
		t.Fatalf("machine and transition table drifted:\nstored %v\ncoded  %v", stored, coded)
	}
}

func TestPostgresEveryNamedConstraintHasAnErrorMapping(t *testing.T) {
	content, err := os.ReadFile(migrationRelativePath)
	if err != nil {
		t.Fatal(err)
	}
	registered := make(map[string]struct{})
	for _, name := range adapters.ListRegisteredConstraintNames() {
		registered[name] = struct{}{}
	}
	pattern := regexp.MustCompile(`CONSTRAINT ([a-z0-9_]+)|CONSTRAINT = '([a-z0-9_]+)'`)
	for _, match := range pattern.FindAllStringSubmatch(string(content), -1) {
		name := match[1] + match[2]
		if _, ok := registered[name]; !ok {
			t.Fatalf("constraint %s has no entry in the error registry", name)
		}
	}
}

func TestPostgresMigrationIsIdempotentAndReversible(t *testing.T) {
	fixture := newPostgresFixture(t)
	applyFile(t, fixture.owner, migrationRelativePath)
	if countRows(t, fixture.owner, "SELECT count(*) FROM tenant_status_transition") != 5 {
		t.Fatal("re-applying the migration must not duplicate seed rows")
	}
	applyFile(t, fixture.owner, rollbackRelativePath)
	if countRows(t, fixture.owner, "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'tenant_%'") != 0 {
		t.Fatal("rollback must remove every tenant isolation table")
	}
	applyFile(t, fixture.owner, rollbackRelativePath)
	applyFile(t, fixture.owner, migrationRelativePath)
	if countRows(t, fixture.owner, "SELECT count(*) FROM tenant_status") != 4 {
		t.Fatal("migration must re-apply after rollback")
	}
}

func TestPostgresNegativeConstraintMatrix(t *testing.T) {
	fixture := newPostgresFixture(t)
	platformCreate(t, fixture, "tenant-a", 5)
	insertRegistry := "INSERT INTO tenant_registry (id, public_id, tenant_id, correlation_id, status, max_concurrent_sagas) VALUES ($1, $2, $3, $4, $5, $6)"
	validPublic := "tnt_" + "abcdefghijklmnopqrstuvwxyz"
	cases := []struct {
		name       string
		statement  string
		args       []any
		constraint string
	}{
		{"tenant id uppercase", insertRegistry, []any{uuid.New(), validPublic, "Tenant-B", uuid.New(), "ACTIVE", 5}, "tenant_registry_tenant_id_format_ck"},
		{"tenant id too short", insertRegistry, []any{uuid.New(), validPublic, "ab", uuid.New(), "ACTIVE", 5}, "tenant_registry_tenant_id_format_ck"},
		{"public id prefix", insertRegistry, []any{uuid.New(), "xxx_abcdefghijklmnopqrstuvwxyz", "tenant-b", uuid.New(), "ACTIVE", 5}, "tenant_registry_public_id_ck"},
		{"max sagas zero", insertRegistry, []any{uuid.New(), validPublic, "tenant-b", uuid.New(), "ACTIVE", 0}, "tenant_registry_max_concurrent_sagas_ck"},
		{"max sagas above bound", insertRegistry, []any{uuid.New(), validPublic, "tenant-b", uuid.New(), "ACTIVE", 1000001}, "tenant_registry_max_concurrent_sagas_ck"},
		{"unknown status is refused as an initial status", insertRegistry, []any{uuid.New(), validPublic, "tenant-b", uuid.New(), "DELETED", 5}, "tenant_registry_initial_status_ck"},
		{"initial status must be active", insertRegistry, []any{uuid.New(), validPublic, "tenant-b", uuid.New(), "SUSPENDED", 5}, "tenant_registry_initial_status_ck"},
		{"duplicate tenant id", insertRegistry, []any{uuid.New(), validPublic, "tenant-a", uuid.New(), "ACTIVE", 5}, "tenant_registry_tenant_id_uk"},
		{"suspension reason without suspension", "UPDATE tenant_registry SET suspension_reason = 'BILLING' WHERE tenant_id = 'tenant-a'", nil, "tenant_registry_suspension_reason_ck"},
		{"suspended without reason", "UPDATE tenant_registry SET status = 'SUSPENDED' WHERE tenant_id = 'tenant-a'", nil, "tenant_registry_suspension_reason_ck"},
		{"illegal transition", "UPDATE tenant_registry SET status = 'OFFBOARDED' WHERE tenant_id = 'tenant-a'", nil, "tenant_registry_status_transition_ck"},
		{"tenant id is immutable", "UPDATE tenant_registry SET tenant_id = 'tenant-z' WHERE tenant_id = 'tenant-a'", nil, "tenant_registry_tenant_id_immutable_ck"},
		{"public id is immutable", "UPDATE tenant_registry SET public_id = 'tnt_zzzzzzzzzzzzzzzzzzzzzzzzzz' WHERE tenant_id = 'tenant-a'", nil, "tenant_registry_public_id_immutable_ck"},
		{"registry rows are never deleted", "DELETE FROM tenant_registry WHERE tenant_id = 'tenant-a'", nil, "tenant_registry_append_only_ck"},
		{"quota count negative", "UPDATE tenant_saga_quota SET active_saga_count = -1 WHERE tenant_id = 'tenant-a'", nil, "tenant_saga_quota_active_saga_count_ck"},
		{"quota tenant immutable", "UPDATE tenant_saga_quota SET tenant_id = 'tenant-z' WHERE tenant_id = 'tenant-a'", nil, "tenant_saga_quota_tenant_id_immutable_ck"},
		{"quota requires registered tenant", "INSERT INTO tenant_saga_quota (tenant_id) VALUES ('tenant-ghost')", nil, "tenant_saga_quota_tenant_id_fk"},
		{"tombstone is append only", "INSERT INTO tenant_tombstone (tenant_id) VALUES ('tenant-a'); DELETE FROM tenant_tombstone", nil, "tenant_tombstone_append_only_ck"},
		{"transition table rejects self loop", "INSERT INTO tenant_status_transition VALUES ('ACTIVE','ACTIVE')", nil, "tenant_status_transition_distinct_ck"},
	}
	for _, tc := range cases {
		_, err := fixture.owner.Exec(tc.statement, tc.args...)
		if constraintOf(err) != tc.constraint {
			t.Fatalf("%s: constraint = %q, want %q (err %v)", tc.name, constraintOf(err), tc.constraint, err)
		}
	}
}

func TestPostgresQuarantineAndSecurityEventConstraints(t *testing.T) {
	fixture := newPostgresFixture(t)
	platformCreate(t, fixture, "tenant-a", 5)
	quarantine := "INSERT INTO tenant_message_quarantine (id, public_id, correlation_id, source_topic, source_partition, source_offset, reason, status, payload_sha256, resolved_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	digest, public := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "tqm_abcdefghijklmnopqrstuvwxyz"
	cases := []struct {
		name       string
		statement  string
		args       []any
		constraint string
	}{
		{"invalid reason", quarantine, []any{uuid.New(), public, uuid.New(), "t", 0, 0, "OTHER", "PENDING", digest, nil}, "tenant_message_quarantine_reason_ck"},
		{"invalid status", quarantine, []any{uuid.New(), public, uuid.New(), "t", 0, 0, "UNKNOWN_TENANT", "DONE", digest, "2026-01-01T00:00:00Z"}, "tenant_message_quarantine_status_ck"},
		{"uppercase digest", quarantine, []any{uuid.New(), public, uuid.New(), "t", 0, 0, "UNKNOWN_TENANT", "PENDING", "ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789", nil}, "tenant_message_quarantine_payload_sha256_ck"},
		{"negative offset", quarantine, []any{uuid.New(), public, uuid.New(), "t", 0, -1, "UNKNOWN_TENANT", "PENDING", digest, nil}, "tenant_message_quarantine_source_offset_ck"},
		{"pending with resolved_at", quarantine, []any{uuid.New(), public, uuid.New(), "t", 0, 0, "UNKNOWN_TENANT", "PENDING", digest, "2026-01-01T00:00:00Z"}, "tenant_message_quarantine_resolved_ck"},
		{"released without resolved_at", quarantine, []any{uuid.New(), public, uuid.New(), "t", 0, 0, "UNKNOWN_TENANT", "RELEASED", digest, nil}, "tenant_message_quarantine_resolved_ck"},
		{"blank topic", quarantine, []any{uuid.New(), public, uuid.New(), " ", 0, 0, "UNKNOWN_TENANT", "PENDING", digest, nil}, "tenant_message_quarantine_source_topic_ck"},
		{"security event type", "INSERT INTO tenant_security_event (id, public_id, actor_tenant_id, correlation_id, event_type, resource_kind, occurred_at) VALUES ($1, 'tse_abcdefghijklmnopqrstuvwxyz', 'tenant-a', $2, 'OTHER', 'saga', now())", []any{uuid.New(), uuid.New()}, "tenant_security_event_type_ck"},
		{"security event actor must be registered", "INSERT INTO tenant_security_event (id, public_id, actor_tenant_id, correlation_id, event_type, resource_kind, occurred_at) VALUES ($1, 'tse_abcdefghijklmnopqrstuvwxyz', 'tenant-ghost', $2, 'CROSS_TENANT_REFERENCE', 'saga', now())", []any{uuid.New(), uuid.New()}, "tenant_security_event_actor_tenant_id_fk"},
	}
	for _, tc := range cases {
		_, err := fixture.owner.Exec(tc.statement, tc.args...)
		if constraintOf(err) != tc.constraint {
			t.Fatalf("%s: constraint = %q, want %q (err %v)", tc.name, constraintOf(err), tc.constraint, err)
		}
	}
	insertEvent := "INSERT INTO tenant_security_event (id, public_id, actor_tenant_id, correlation_id, event_type, resource_kind, occurred_at) VALUES ($1, 'tse_abcdefghijklmnopqrstuvwxyz', 'tenant-a', $2, 'CROSS_TENANT_REFERENCE', 'saga', now())"
	if _, err := fixture.owner.Exec(insertEvent, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.owner.Exec("UPDATE tenant_security_event SET resource_kind = 'x'"); constraintOf(err) != "tenant_security_event_append_only_ck" {
		t.Fatalf("security events must be append-only even for the owner, got %v", err)
	}
}

func TestPostgresRowLevelSecurityDeniesWithoutContextAndNeverLeaks(t *testing.T) {
	fixture := newPostgresFixture(t)
	platformCreate(t, fixture, "tenant-a", 5)
	platformCreate(t, fixture, "tenant-b", 5)
	ctx := context.Background()
	conn, err := fixture.app.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var visible int
	if err = conn.QueryRowContext(ctx, "SELECT count(*) FROM tenant_saga_quota").Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("autocommit statement without tenant context must see no rows, saw %d (%v)", visible, err)
	}
	if _, err = conn.ExecContext(ctx, "SET LOCAL app.tenant_id = 'tenant-a'"); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRowContext(ctx, "SELECT count(*) FROM tenant_saga_quota").Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("SET LOCAL outside a transaction must have no effect, saw %d rows (%v)", visible, err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, queries.FLOW_SET_TENANT_CONTEXT, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM tenant_saga_quota").Scan(&visible); err != nil || visible != 1 {
		t.Fatalf("tenant-a context must see exactly its own row, saw %d (%v)", visible, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRowContext(ctx, "SELECT count(*) FROM tenant_saga_quota").Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("the pooled connection must not retain the previous tenant, saw %d rows (%v)", visible, err)
	}
}

func TestPostgresWithCheckBlocksCrossTenantWrites(t *testing.T) {
	fixture := newPostgresFixture(t)
	platformCreate(t, fixture, "tenant-a", 5)
	platformCreate(t, fixture, "tenant-b", 5)
	ctx := context.Background()
	runAsTenantA := func(statement string, args ...any) error {
		tx, err := fixture.app.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, queries.FLOW_SET_TENANT_CONTEXT, "tenant-a"); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, statement, args...)
		return err
	}
	if err := runAsTenantA("INSERT INTO tenant_message_quarantine (id, public_id, correlation_id, source_topic, source_partition, source_offset, reason, payload_sha256) VALUES ($1, 'tqm_abcdefghijklmnopqrstuvwxyz', $2, 't', 0, 0, 'UNKNOWN_TENANT', $3)", uuid.New(), uuid.New(), "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatalf("quarantine mailbox insert must be allowed for the application role: %v", err)
	}
	foreignEvent := "INSERT INTO tenant_security_event (id, public_id, actor_tenant_id, correlation_id, event_type, resource_kind, occurred_at) VALUES ($1, 'tse_abcdefghijklmnopqrstuvwxyz', 'tenant-b', $2, 'CROSS_TENANT_REFERENCE', 'saga', now())"
	if err := runAsTenantA(foreignEvent, uuid.New(), uuid.New()); sqlStateOf(err) != "42501" {
		t.Fatalf("WITH CHECK must refuse an event for another tenant, got %v", err)
	}
	if err := runAsTenantA("UPDATE tenant_saga_quota SET tenant_id = 'tenant-b' WHERE tenant_id = 'tenant-a'"); err == nil {
		t.Fatal("moving a quota row into another tenant must be refused")
	}
	if err := runAsTenantA("UPDATE tenant_saga_quota SET active_saga_count = 99 WHERE tenant_id = 'tenant-b'"); err != nil {
		t.Fatalf("an UPDATE that matches no visible row is silently empty, got %v", err)
	}
	if countRows(t, fixture.owner, "SELECT count(*) FROM tenant_saga_quota WHERE tenant_id = 'tenant-b' AND active_saga_count = 99") != 0 {
		t.Fatal("tenant-a must not be able to modify tenant-b's quota")
	}
	if err := runAsTenantA("SELECT * FROM tenant_security_event"); sqlStateOf(err) != "42501" {
		t.Fatalf("the application role must not read security events, got %v", err)
	}
	if err := runAsTenantA("UPDATE tenant_registry SET max_concurrent_sagas = 999 WHERE tenant_id = 'tenant-a'"); sqlStateOf(err) != "42501" {
		t.Fatalf("the application role must not write the registry, got %v", err)
	}
}

func TestPostgresQuotaReservationIsAtomicUnderContention(t *testing.T) {
	fixture := newPostgresFixture(t)
	platformCreate(t, fixture, "tenant-a", 10)
	var admitted, refused atomic.Int64
	var group sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := fixture.appSvc.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a")
			if err == nil {
				admitted.Add(1)
			} else if statusCodeOf(err) == http.StatusTooManyRequests {
				refused.Add(1)
			}
		}()
	}
	group.Wait()
	if admitted.Load() != 10 || refused.Load() != 10 {
		t.Fatalf("admitted %d refused %d, want 10 and 10", admitted.Load(), refused.Load())
	}
	if countRows(t, fixture.owner, "SELECT active_saga_count FROM tenant_saga_quota WHERE tenant_id = 'tenant-a'") != 10 {
		t.Fatal("stored counter must equal the admitted reservations")
	}
}

func TestPostgresTenantLifecycleAdmissionAndTombstone(t *testing.T) {
	fixture := newPostgresFixture(t)
	ctx := context.Background()
	record := platformCreate(t, fixture, "tenant-a", 5)
	billing := contracts.SuspensionReasonBilling
	suspended, err := fixture.platSvc.Registry.UpdateTenantStatus(ctx, contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: record.Version, TargetStatus: contracts.TenantStatusSuspended, SuspensionReason: &billing})
	if err != nil || suspended.Version != record.Version+1 || suspended.SuspensionReason == nil {
		t.Fatalf("suspend failed: %+v %v", suspended, err)
	}
	if _, err = fixture.platSvc.Registry.UpdateTenantStatus(ctx, contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: record.Version, TargetStatus: contracts.TenantStatusActive}); statusCodeOf(err) != http.StatusPreconditionFailed {
		t.Fatalf("stale version must be 412, got %v", err)
	}
	if _, err = fixture.appSvc.Access.EvaluateCommandAdmission(ctx, contracts.EvaluateAdmissionInput{TenantID: "tenant-a", Kind: contracts.CommandKindNewCommand}); statusCodeOf(err) != http.StatusForbidden {
		t.Fatalf("billing suspension must reject new commands, got %v", err)
	}
	if _, err = fixture.appSvc.Quota.CreateSagaQuotaReservation(ctx, "tenant-a"); statusCodeOf(err) != http.StatusForbidden {
		t.Fatalf("suspended tenant must not reserve capacity, got %v", err)
	}
	version := suspended.Version
	for _, target := range []contracts.TenantStatus{contracts.TenantStatusOffboarding, contracts.TenantStatusOffboarded} {
		updated, updateErr := fixture.platSvc.Registry.UpdateTenantStatus(ctx, contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: version, TargetStatus: target})
		if updateErr != nil {
			t.Fatalf("transition to %s: %v", target, updateErr)
		}
		version = updated.Version
	}
	if countRows(t, fixture.owner, "SELECT count(*) FROM tenant_tombstone WHERE tenant_id = 'tenant-a'") != 1 {
		t.Fatal("offboarding must write a tombstone")
	}
	_, err = fixture.platSvc.Registry.CreateTenant(ctx, contracts.CreateTenantInput{TenantID: "tenant-a", MaxConcurrentSagas: 5, CorrelationID: uuid.New()})
	if statusCodeOf(err) != http.StatusConflict || err.Error() != "tenant id was offboarded and can never be reissued" {
		t.Fatalf("reissue must conflict with the mapped message and no raw database text, got %v", err)
	}
}

func TestPostgresSecurityEventsAndQuarantineEndToEnd(t *testing.T) {
	fixture := newPostgresFixture(t)
	ctx := context.Background()
	platformCreate(t, fixture, "tenant-a", 5)
	requested := "tenant-b"
	if _, err := fixture.appSvc.Access.EvaluateTenantResolution(ctx, contracts.ResolveTenantInput{PrincipalTenantID: "tenant-a", RequestedTenantID: &requested, CorrelationID: uuid.New()}); statusCodeOf(err) != http.StatusNotFound {
		t.Fatalf("foreign tenant selection must be 404, got %v", err)
	}
	resourceID := "saga-1"
	if err := fixture.appSvc.Access.EvaluateCrossTenantReference(ctx, contracts.CrossTenantReferenceInput{ActorTenantID: "tenant-a", ReferencedTenantID: "tenant-b", ResourceKind: "saga", ResourceID: &resourceID, CorrelationID: uuid.New()}); statusCodeOf(err) != http.StatusNotFound {
		t.Fatalf("cross tenant reference must be 404, got %v", err)
	}
	if countRows(t, fixture.owner, "SELECT count(*) FROM tenant_security_event WHERE actor_tenant_id = 'tenant-a'") != 2 {
		t.Fatal("both probes must be recorded as security events")
	}

	header, payload := "tenant-a", "tenant-b"
	mismatch := messageEnvelope(&header, &header, &payload, nil)
	if decision, err := fixture.appSvc.Quarantine.EvaluateMessageTenant(ctx, mismatch); err != nil || decision.Proceed {
		t.Fatalf("mismatch must quarantine, got %+v %v", decision, err)
	}
	early := envelopeForTenant("tenant-early", 5)
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := fixture.appSvc.Quarantine.EvaluateMessageTenant(ctx, early); err != nil {
			t.Fatal(err)
		}
	}
	if countRows(t, fixture.owner, "SELECT count(*) FROM tenant_message_quarantine WHERE status = 'PENDING'") != 2 {
		t.Fatal("redelivery must not duplicate quarantine rows")
	}
	if countRows(t, fixture.owner, "SELECT count(*) FROM tenant_security_event WHERE event_type = 'MESSAGE_INTEGRITY_MISMATCH'") != 1 {
		t.Fatal("an integrity mismatch for a known tenant must raise a security alert row")
	}
	platformCreate(t, fixture, "tenant-early", 5)
	released, err := fixture.platSvc.Quarantine.UpdateQuarantinedMessagesRelease(ctx, contracts.ReleaseQuarantinedMessagesInput{TenantID: "tenant-early"})
	if err != nil || len(released) != 1 || released[0].Status != contracts.QuarantineStatusReleased {
		t.Fatalf("expected one released record, got %+v %v", released, err)
	}
	if decision, evalErr := fixture.appSvc.Quarantine.EvaluateMessageTenant(ctx, envelopeForTenant("tenant-early", 6)); evalErr != nil || !decision.Proceed {
		t.Fatalf("registered tenant must proceed, got %+v %v", decision, evalErr)
	}
}

func TestPostgresQuotaReconciliationAndInvariantQueries(t *testing.T) {
	fixture := newPostgresFixture(t)
	ctx := context.Background()
	platformCreate(t, fixture, "tenant-a", 5)
	for reservation := 0; reservation < 3; reservation++ {
		if _, err := fixture.appSvc.Quota.CreateSagaQuotaReservation(ctx, "tenant-a"); err != nil {
			t.Fatal(err)
		}
	}
	quota, err := fixture.appSvc.Quota.GetSagaQuota(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := fixture.appSvc.Quota.UpdateSagaQuotaCounter(ctx, contracts.UpdateSagaQuotaCounterInput{TenantID: "tenant-a", ExpectedVersion: quota.Version, ObservedActiveSagas: 1})
	if err != nil || repaired.ActiveSagaCount != 1 {
		t.Fatalf("reconciliation failed: %+v %v", repaired, err)
	}
	if _, err = fixture.appSvc.Quota.UpdateSagaQuotaCounter(ctx, contracts.UpdateSagaQuotaCounterInput{TenantID: "tenant-a", ExpectedVersion: quota.Version, ObservedActiveSagas: 0}); statusCodeOf(err) != http.StatusPreconditionFailed {
		t.Fatalf("stale reconciliation must be 412, got %v", err)
	}
	if err = fixture.appSvc.Quota.DeleteSagaQuotaReservation(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err = fixture.appSvc.Quota.DeleteSagaQuotaReservation(ctx, "tenant-a"); statusCodeOf(err) != http.StatusConflict {
		t.Fatalf("release below zero must conflict, got %v", err)
	}
	for _, query := range []string{queries.FLOW_CHECK_TENANT_REGISTRY_QUOTA_ROW_MISSING, queries.FLOW_CHECK_TENANT_SAGA_QUOTA_OVER_LIMIT} {
		if rows := countViolations(t, fixture.owner, query); rows != 0 {
			t.Fatalf("invariant query reported %d violations", rows)
		}
	}
	if rows := countViolations(t, fixture.owner, queries.FLOW_CHECK_TENANT_REGISTRY_TOMBSTONE_MISSING, contracts.TenantStatusOffboarded); rows != 0 {
		t.Fatalf("tombstone invariant reported %d violations", rows)
	}
}

func countViolations(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("invariant query: %v", err)
	}
	defer rows.Close()
	violations := 0
	for rows.Next() {
		violations++
	}
	return violations
}

func TestPostgresInvariantQueriesDetectSeededViolations(t *testing.T) {
	fixture := newPostgresFixture(t)
	platformCreate(t, fixture, "tenant-a", 5)
	if _, err := fixture.owner.Exec("DELETE FROM tenant_saga_quota WHERE tenant_id = 'tenant-a'"); err != nil {
		t.Fatal(err)
	}
	if countViolations(t, fixture.owner, queries.FLOW_CHECK_TENANT_REGISTRY_QUOTA_ROW_MISSING) != 1 {
		t.Fatal("the missing quota row must be reported")
	}
}

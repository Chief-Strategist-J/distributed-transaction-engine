package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	adapters "distributed-transaction-engine/src/features/compensation-engine/infrastructure/adapters"
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

func TestPostgresCompensationAdapter_RealDatabaseInvariants(t *testing.T) {
	db := getRealPostgresDB(t)
	defer db.Close()

	ctx := context.Background()
	adapter := adapters.NewPostgresCompensationRepositoryAdapter(db)

	tenantID := fmt.Sprintf("tenant_cmp_%d", time.Now().UnixNano())
	sagaID := uuid.New().String()
	planID := uuid.New().String()
	planPubID := "cmp_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:26]

	_, err := db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, 'wfd_01HXYZ0123456789ABCDEF0123', $2, 'cmp-test-wf', 1, 'TEST', 'ext_cmp_999',
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
			'hash_placeholder', 'SYSTEM', 'ext_init_cmp', 'in_hash', $4,
			'COMPENSATING', 1, 0, 1, 'FORWARD',
			false, 1, NOW() + INTERVAL '1 day', '{}', '{}',
			NOW(), NOW()
		)
	`, sagaID, "seq_"+strings.ReplaceAll(uuid.New().String(), "-", "")[:26], tenantID, uuid.New().String())
	if err != nil {
		t.Fatalf("failed to seed saga instance: %v", err)
	}

	now := time.Now().UTC()
	plan := &contracts.SagaCompensationPlanInstance{
		ID:           planID,
		PublicID:     planPubID,
		TenantID:     tenantID,
		SagaID:       sagaID,
		CurrentState: contracts.PlanStateCompensating,
		TotalSteps:   1,
		Version:      1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	steps := []contracts.SagaStepCompensationInstance{
		{
			ID:                  uuid.New().String(),
			PublicID:            fmt.Sprintf("cst_%026d", time.Now().UnixNano()%10000000000),
			TenantID:            tenantID,
			PlanID:              planID,
			SagaID:              sagaID,
			StepID:              "step_flight_reserve",
			StepName:            "Reserve Flight Ticket",
			ExecutionOrder:      1,
			Status:              contracts.StepStatusPending,
			ForwardExecutionID:  "fwd_exec_101",
			CompensationAction:  "HTTP_CANCEL_TICKET",
			CompensationPayload: map[string]interface{}{"pnr": "PNR-1234"},
			ResidualPayload:     make(map[string]interface{}),
			IdempotencyKey:      fmt.Sprintf("%s:flight:compensate", sagaID),
			Version:             1,
			CreatedAt:           now,
			UpdatedAt:           now,
		},
	}

	if err := adapter.CreateCompensationPlanAtomic(ctx, plan, steps); err != nil {
		t.Fatalf("CreateCompensationPlanAtomic failed: %v", err)
	}

	duplicateErr := adapter.CreateCompensationPlanAtomic(ctx, plan, steps)
	if duplicateErr == nil {
		t.Fatal("expected duplicate compensation plan error, got nil")
	}

	fetched, err := adapter.GetCompensationPlan(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("GetCompensationPlan failed: %v", err)
	}
	if fetched.PublicID != planPubID || len(fetched.Steps) != 1 {
		t.Fatalf("unexpected fetched plan: %+v", fetched)
	}

	if err := adapter.UpdateStepOutcome(ctx, tenantID, planID, "step_flight_reserve", contracts.StepStatusCompensated, nil, ""); err != nil {
		t.Fatalf("UpdateStepOutcome failed: %v", err)
	}

	if err := adapter.UpdatePlanCAS(ctx, tenantID, planID, 1, contracts.PlanStateFullyCompensated, 1, 0, nil); err != nil {
		t.Fatalf("UpdatePlanCAS failed: %v", err)
	}

	staleErr := adapter.UpdatePlanCAS(ctx, tenantID, planID, 1, contracts.PlanStateCompensatingNeedsAttention, 1, 0, nil)
	if staleErr == nil {
		t.Fatal("expected stale CAS update to fail, got nil")
	}
}

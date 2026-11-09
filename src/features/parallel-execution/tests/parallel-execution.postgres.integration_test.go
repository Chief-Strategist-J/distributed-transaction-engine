package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	adapters "distributed-transaction-engine/src/features/parallel-execution/infrastructure/adapters"
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

func TestPostgresParallelExecutionAdapter_RealDatabaseInvariants(t *testing.T) {
	db := getRealPostgresDB(t)
	defer db.Close()

	ctx := context.Background()
	adapter := adapters.NewPostgresParallelExecutionRepositoryAdapter(db)

	tenantID := fmt.Sprintf("tenant_par_%d", time.Now().UnixNano())
	workflowDefID := uuid.New().String()
	sagaID := uuid.New().String()
	groupID := uuid.New().String()
	groupPubID := fmt.Sprintf("par_%026d", time.Now().Unix()%10000000000)

	_, err := db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, 'wfd_01HXYZ0123456789ABCDEF0123', $2, 'real-test-workflow', 1, 'TEST', 'ext_par_999',
			'11111111-1111-4111-8111-111111111111', 'hash_real', '{"steps":[]}', 'ACTIVE', 3, NOW(), NOW()
		) ON CONFLICT DO NOTHING;
	`, workflowDefID, tenantID)
	if err != nil {
		t.Fatalf("failed to insert workflow definition: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO saga_instances (
			id, public_id, tenant_id, workflow_definition_id, workflow_version,
			spec_hash, source, external_id, input_hash, correlation_id,
			current_state, version, current_step_index, total_steps, execution_phase,
			pivot_reached, owner_epoch, deadline_at, initial_payload, final_output,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, 1,
			'hash_placeholder', 'SYSTEM', 'ext_init_1', 'in_hash', $5,
			'RUNNING', 1, 0, 1, 'FORWARD',
			false, 1, NOW() + INTERVAL '1 day', '{}', '{}',
			NOW(), NOW()
		)
	`, sagaID, fmt.Sprintf("seq_%026d", time.Now().Unix()%10000000000), tenantID, workflowDefID, uuid.New().String())
	if err != nil {
		t.Fatalf("failed to insert saga instance: %v", err)
	}

	now := time.Now().UTC()
	group := &contracts.ParallelGroupInstance{
		ID:            groupID,
		PublicID:      groupPubID,
		TenantID:      tenantID,
		SagaID:        sagaID,
		JoinPolicy:    contracts.PolicyAllRequired,
		QuorumCount:   0,
		CurrentState:  contracts.GroupStateFanoutActive,
		TotalBranches: 2,
		Version:       1,
		CorrelationID: uuid.New().String(),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	branches := []contracts.ParallelBranchInstance{
		{
			ID:             uuid.New().String(),
			PublicID:       fmt.Sprintf("pbr_%026d", 1),
			TenantID:       tenantID,
			GroupID:        groupID,
			SagaID:         sagaID,
			BranchID:       "branch-inventory",
			BranchName:     "Reserve Inventory",
			Role:           contracts.RoleRequired,
			EffectType:     contracts.EffectCompensable,
			CurrentState:   contracts.BranchStateDispatched,
			IdempotencyKey: fmt.Sprintf("%s:inv:1", groupID),
			BranchInput:    map[string]interface{}{"item": "keyboard"},
			BranchOutput:   make(map[string]interface{}),
			CorrelationID:  group.CorrelationID,
			Version:        1,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			ID:             uuid.New().String(),
			PublicID:       fmt.Sprintf("pbr_%026d", 2),
			TenantID:       tenantID,
			GroupID:        groupID,
			SagaID:         sagaID,
			BranchID:       "branch-payment",
			BranchName:     "Charge Card",
			Role:           contracts.RoleRequired,
			EffectType:     contracts.EffectCompensable,
			CurrentState:   contracts.BranchStateDispatched,
			IdempotencyKey: fmt.Sprintf("%s:pay:1", groupID),
			BranchInput:    map[string]interface{}{"amount": 100},
			BranchOutput:   make(map[string]interface{}),
			CorrelationID:  group.CorrelationID,
			Version:        1,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}

	if err := adapter.CreateGroupAtomic(ctx, group, branches); err != nil {
		t.Fatalf("CreateGroupAtomic failed: %v", err)
	}

	fetchedGroup, err := adapter.GetGroup(ctx, tenantID, groupID)
	if err != nil {
		t.Fatalf("GetGroup failed: %v", err)
	}
	if fetchedGroup.PublicID != groupPubID || len(fetchedGroup.Branches) != 2 {
		t.Fatalf("unexpected fetched group: %+v", fetchedGroup)
	}

	out := map[string]interface{}{"charged": true}
	if err := adapter.UpdateBranchOutcome(ctx, tenantID, groupID, "branch-payment", contracts.BranchStateSucceeded, out, ""); err != nil {
		t.Fatalf("UpdateBranchOutcome failed: %v", err)
	}

	if err := adapter.UpdateGroupCAS(ctx, tenantID, groupID, 1, contracts.GroupStateJoinSucceeded, 2, 0); err != nil {
		t.Fatalf("UpdateGroupCAS failed: %v", err)
	}

	err = adapter.UpdateGroupCAS(ctx, tenantID, groupID, 1, contracts.GroupStateCompensating, 2, 0)
	if err == nil {
		t.Fatal("expected stale CAS update to fail with conflict error, got nil")
	}
}

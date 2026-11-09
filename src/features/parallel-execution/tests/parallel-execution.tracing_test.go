package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	machines "distributed-transaction-engine/src/features/parallel-execution/domain/machines"
	adapters "distributed-transaction-engine/src/features/parallel-execution/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/parallel-execution/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestParallelExecution_EndToEndTracingAndRealDB(t *testing.T) {
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
		t.Skipf("skipping: postgres not reachable at %s:%s", host, port)
	}

	ctx := context.Background()
	tenantID := fmt.Sprintf("tenant_e2e_%d", time.Now().UnixNano())
	workflowDefID := uuid.New().String()
	sagaID := uuid.New().String()
	groupID := uuid.New().String()
	groupPubID := fmt.Sprintf("par_%026d", time.Now().Unix()%10000000000)

	_, err = db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, 'wfd_01HXYZ0123456789ABCDEF0123', $2, 'parallel-e2e-wf', 1, 'TEST', 'ext_par_e2e',
			'11111111-1111-4111-8111-111111111111', 'hash_real', '{"steps":[]}', 'ACTIVE', 2, NOW(), NOW()
		) ON CONFLICT DO NOTHING;
	`, workflowDefID, tenantID)
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
			$1, $2, $3, $4, 1,
			'hash_placeholder', 'SYSTEM', 'ext_init_e2e', 'in_hash', $5,
			'RUNNING', 1, 0, 1, 'FORWARD',
			false, 1, NOW() + INTERVAL '1 day', '{}', '{}',
			NOW(), NOW()
		)
	`, sagaID, fmt.Sprintf("seq_%026d", time.Now().Unix()%10000000000), tenantID, workflowDefID, uuid.New().String())
	if err != nil {
		t.Fatalf("failed to seed saga instance: %v", err)
	}

	repo := adapters.NewPostgresParallelExecutionRepositoryAdapter(db)
	dispatcher := adapters.NewMemoryParallelDispatcherAdapter()
	machine := machines.NewParallelExecutionMachine()
	tracer := observability.NewTracer("parallel-execution-test")

	svc := service.NewParallelExecutionService(service.ParallelExecutionServiceDeps{
		Repo:       repo,
		Dispatcher: dispatcher,
		Machine:    machine,
		Tracer:     tracer,
	})

	input := &contracts.StartParallelGroupInput{
		ID:          groupID,
		PublicID:    groupPubID,
		TenantID:    tenantID,
		SagaID:      sagaID,
		JoinPolicy:  contracts.PolicyAllRequired,
		QuorumCount: 0,
		Branches: []contracts.BranchDefinitionInput{
			{
				BranchID:   "branch-credit-check",
				Name:       "Check Credit Score",
				Role:       contracts.RoleRequired,
				EffectType: contracts.EffectCompensable,
				Payload:    map[string]interface{}{"score": 750},
			},
			{
				BranchID:   "branch-fraud-check",
				Name:       "Risk & Fraud Evaluation",
				Role:       contracts.RoleRequired,
				EffectType: contracts.EffectCompensable,
				Payload:    map[string]interface{}{"riskLevel": "LOW"},
			},
		},
	}

	group, err := svc.StartParallelGroup(ctx, input)
	if err != nil {
		t.Fatalf("StartParallelGroup failed: %v", err)
	}
	if group.CurrentState != contracts.GroupStateFanoutActive || len(group.Branches) != 2 {
		t.Fatalf("unexpected group state: %+v", group)
	}

	err = svc.HandleBranchCompletion(ctx, tenantID, groupID, "branch-credit-check", true, map[string]interface{}{"scoreApproved": true}, "")
	if err != nil {
		t.Fatalf("HandleBranchCompletion 1 failed: %v", err)
	}

	fetchedGroup, err := svc.GetGroup(ctx, tenantID, groupID)
	if err != nil {
		t.Fatalf("GetGroup failed: %v", err)
	}
	if fetchedGroup.CurrentState != contracts.GroupStateFanoutActive {
		t.Fatalf("expected FANOUT_ACTIVE after single branch, got %s", fetchedGroup.CurrentState)
	}

	err = svc.HandleBranchCompletion(ctx, tenantID, groupID, "branch-fraud-check", true, map[string]interface{}{"fraudApproved": true}, "")
	if err != nil {
		t.Fatalf("HandleBranchCompletion 2 failed: %v", err)
	}

	completedGroup, err := svc.GetGroup(ctx, tenantID, groupID)
	if err != nil {
		t.Fatalf("GetGroup after completion failed: %v", err)
	}
	if completedGroup.CurrentState != contracts.GroupStateJoinSucceeded {
		t.Fatalf("expected JOIN_SUCCEEDED, got %s", completedGroup.CurrentState)
	}
	if completedGroup.SucceededBranches != 2 {
		t.Fatalf("expected 2 succeeded branches, got %d", completedGroup.SucceededBranches)
	}
}

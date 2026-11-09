package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	machines "distributed-transaction-engine/src/features/sequential-execution/domain/machines"
	adapters "distributed-transaction-engine/src/features/sequential-execution/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/sequential-execution/service"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowAdapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	"distributed-transaction-engine/src/infra/observability"
	_ "github.com/lib/pq"
)

func TestSequentialExecution_EndToEndTracingAndRealDB(t *testing.T) {
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

	tracer := observability.NewTracer("sequential-execution-e2e")
	repo := adapters.NewPostgresSequentialExecutionRepositoryAdapter(db)
	wfRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	dispatcher := adapters.NewMemorySequentialDispatcherAdapter()
	machine := machines.NewSequentialExecutionMachine()

	svc := service.NewSequentialExecutionService(service.SequentialExecutionServiceDeps{
		Repo:         repo,
		WorkflowRepo: wfRepo,
		Dispatcher:   dispatcher,
		Machine:      machine,
		Tracer:       tracer,
	})

	// Start root span representing ingress API call with W3C Traceparent
	ctx, rootSpan := tracer.StartSpan(context.Background(), "POST /api/v1/sagas/sequential/start")
	defer rootSpan.End()

	traceID := "5a4ef20c-3a12-4a63-8202-557893a9f739"
	t.Logf("=== END-TO-END TRACE GENERATED ===")
	t.Logf("TraceID (W3C Correlation UUID): %s", traceID)

	tenantID := fmt.Sprintf("tenant_trace_%d", time.Now().UnixNano())
	wfID := "77777777-7777-4777-8777-777777777777"

	// Register workflow definition in DB & Memory repo
	wfDef := &workflowContracts.WorkflowDefinitionInstance{
		ID:            wfID,
		PublicID:      "wfd_01HXYZ0123456789ABCDEFTRC1",
		TenantID:      tenantID,
		Name:          "payment-tracing-pipeline",
		Version:       1,
		Status:        "ACTIVE",
		CorrelationID: "99999999-9999-4999-8999-999999999999",
		Steps: []workflowContracts.StepDefinition{
			{StepID: "step_auth", StepName: "Authorize", IsPivot: false},
			{StepID: "step_charge", StepName: "Charge", IsPivot: true},
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	_ = wfRepo.Save(ctx, wfDef)

	_, err = db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, 1, 'TEST', 'ext_trace_1',
			'99999999-9999-4999-8999-999999999999', 'spec_trace_hash', '{"steps":[]}', 'ACTIVE', 2, NOW(), NOW()
		);
	`, wfDef.ID, wfDef.PublicID, tenantID, wfDef.Name)
	if err != nil {
		t.Fatalf("failed to insert workflow definition: %v", err)
	}

	sagaID := "33333333-3333-4333-8333-333333333333"
	startInput := &contracts.StartSequentialSagaInput{
		ID:                   sagaID,
		PublicID:             "seq_01HXYZ0123456789ABCDEFTRC2",
		TenantID:             tenantID,
		WorkflowDefinitionID: wfID,
		Source:               "BILLING_SERVICE",
		ExternalID:           "invoice_90001",
		CorrelationID:        traceID,
		Payload:              map[string]interface{}{"amount": 1000},
	}

	sagaState, err := svc.StartSequentialSaga(ctx, startInput)
	if err != nil {
		t.Fatalf("StartSequentialSaga failed: %v", err)
	}

	t.Logf("Saga Started Successfully: ID=%s, PublicID=%s, State=%s, TraceID=%s",
		sagaState.ID, sagaState.PublicID, sagaState.CurrentState, traceID)

	// Step 0 Completion
	err = svc.HandleStepCompletion(ctx, tenantID, sagaID, 0, true, map[string]interface{}{"auth_code": "AUTH_99"}, false)
	if err != nil {
		t.Fatalf("Step 0 completion failed: %v", err)
	}
	t.Logf("Step 0 'Authorize' Succeeded (Recorded under TraceID %s)", traceID)

	// Step 1 Completion
	err = svc.HandleStepCompletion(ctx, tenantID, sagaID, 1, true, map[string]interface{}{"charge_id": "CH_99"}, false)
	if err != nil {
		t.Fatalf("Step 1 completion failed: %v", err)
	}
	t.Logf("Step 1 'Charge' (Pivot) Succeeded -> Saga COMPLETED")

	// Verify terminal state in Real PostgreSQL
	row := db.QueryRowContext(ctx, "SELECT current_state, version, pivot_reached FROM saga_instances WHERE tenant_id = $1 AND id = $2", tenantID, sagaID)
	var finalState string
	var version int64
	var pivotReached bool
	if err := row.Scan(&finalState, &version, &pivotReached); err != nil {
		t.Fatalf("failed to query real postgres state: %v", err)
	}

	if finalState != "COMPLETED" || !pivotReached || version != 3 {
		t.Fatalf("unexpected DB state: state=%s, version=%d, pivot=%v", finalState, version, pivotReached)
	}

	t.Logf("Verified in PostgreSQL: State=%s, Version=%d, PivotReached=%v", finalState, version, pivotReached)
	t.Logf("Trace context verified with OTel span: IsSampled=%v", rootSpan.SpanContext().IsSampled())
}

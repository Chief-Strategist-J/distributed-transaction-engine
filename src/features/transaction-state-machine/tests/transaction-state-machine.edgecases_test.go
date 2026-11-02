package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
)

func createEdgeCaseFixture(t *testing.T, caseNum int) (*transactionStateMachine.Service, string, string, context.Context) {
	t.Helper()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)
	tenantID := fmt.Sprintf("tenant_case_%02d", caseNum)
	sagaID := fmt.Sprintf("018d9f1c-7201-7000-8000-%012d", caseNum)
	ctx := context.Background()

	publicID := fmt.Sprintf("sag_case%02d000000000000000000", caseNum) // sag_ (4) + 22 chars = 26 chars
	if len(publicID) != 30 { // "sag_" (4) + 26 chars = 30 chars
		publicID = fmt.Sprintf("sag_00000000000000000000%06d", caseNum)
	}
	_, err := svc.InitializeState(ctx, contracts.InitializeStateInput{
		ID:            sagaID,
		PublicID:      publicID,
		TenantID:      tenantID,
		Source:        "edge-case-suite",
		ExternalID:    fmt.Sprintf("ext_case_%02d", caseNum),
		CorrelationID: fmt.Sprintf("018d9f1c-7201-7000-9000-%012d", caseNum),
	})
	if err != nil {
		t.Fatalf("failed to init case %d: %v", caseNum, err)
	}
	return svc, tenantID, sagaID, ctx
}

func TestEdgeCase01_WorkerClockSkewLeaseEvaluation(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 1)
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "worker A claim",
	})
	if err != nil || rec.CurrentState != contracts.StateRunning {
		t.Fatalf("claim failed: %v", err)
	}
	_, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 0, Reason: "stale worker with skew token",
	})
	if err == nil {
		t.Fatalf("expected rejection for lower fencing token (worker clock skew)")
	}
}

func TestEdgeCase02_DatabaseFailoverDuringTransition(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 2)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	currentState, err := svc.GetState(ctx, tenantID, sagaID)
	if err != nil || currentState.Version != rec.Version {
		t.Fatalf("expected state recovery after simulated failover")
	}
}

func TestEdgeCase03_IdempotencyCacheMissFallback(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 3)
	state1, err := svc.GetState(ctx, tenantID, sagaID)
	if err != nil || state1.CurrentState != contracts.StateCreated {
		t.Fatalf("database fallback lookup failed: %v", err)
	}
}

func TestEdgeCase04_DeadlockResolutionViaOptimisticJitter(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 4)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})

	var wg sync.WaitGroup
	conflicts := 0
	var mu sync.Mutex
	for i := 0; i < 2; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			_, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
				TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
				ExpectedVersion: rec.Version, FencingToken: int64(workerID + 2), Reason: "concurrent CAS update",
			})
			if err != nil {
				mu.Lock()
				conflicts++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if conflicts != 1 {
		t.Fatalf("expected exactly 1 optimistic conflict in deadlock race, got %d", conflicts)
	}
}

func TestEdgeCase05_WorkflowDefinitionVersionImmutable(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 5)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "executed pinned v1 step",
	})
	if rec.CurrentState != contracts.StateRunning {
		t.Fatalf("expected RUNNING on pinned workflow version")
	}
}

func TestEdgeCase06_DownstreamOutageCircuitBreakerTrip(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 6)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "circuit breaker tripped; fast fail",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected immediate transition to COMPENSATING: %v", err)
	}
}

func TestEdgeCase07_RetryBudgetExhaustedShedLoad(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 7)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "global retry budget empty; load shed",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected direct transition to COMPENSATING on budget exhaustion: %v", err)
	}
}

func TestEdgeCase08_DownstreamRateLimitHTTP429(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 8)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventRateLimited,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "HTTP 429 Retry-After: 30",
	})
	if err != nil || rec.CurrentState != contracts.StateThrottledWait {
		t.Fatalf("expected THROTTLED_WAIT on 429: %v", err)
	}
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventThrottleTimerFired,
		ExpectedVersion: rec.Version, FencingToken: 2, Reason: "wake-up timer fired",
	})
	if err != nil || rec.CurrentState != contracts.StateRunning {
		t.Fatalf("expected resume to RUNNING: %v", err)
	}
}

func TestEdgeCase09_LargeSagaChunkedStepProgression(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 9)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	v := rec.Version
	for step := 1; step <= 50; step++ {
		rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
			TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
			ExpectedVersion: v, FencingToken: 1, Reason: fmt.Sprintf("chunked step %d", step),
		})
		v = rec.Version
	}
	if rec.Version != 52 {
		t.Fatalf("expected 52 transitions for chunked execution, got %d", rec.Version)
	}
}

func TestEdgeCase10_LongRunningTransactionWaiting(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 10)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventAsyncSuspend,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "long running hold awaiting escrow title deed (90 days)",
	})
	if err != nil || rec.CurrentState != contracts.StateWaiting {
		t.Fatalf("expected WAITING state for long-running escrow: %v", err)
	}
}

func TestEdgeCase11_TenantIsolationCrossTenantLockout(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 11)
	_, err := svc.GetState(ctx, "malicious_tenant_beta", sagaID)
	if err == nil {
		t.Fatalf("expected not found or cross-tenant query lockout")
	}
	_, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: "malicious_tenant_beta", SagaID: sagaID, Event: contracts.EventClientAbort,
		ExpectedVersion: 1, FencingToken: 1, Reason: "attempt unauthorized cancel",
	})
	if err == nil {
		t.Fatalf("expected rejection for cross-tenant mutation")
	}
	realRecord, _ := svc.GetState(ctx, tenantID, sagaID)
	if realRecord.CurrentState != contracts.StateCreated {
		t.Fatalf("tampering occurred on cross-tenant attempt")
	}
}

func TestEdgeCase12_AuthorizationRevocationMidExecution(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 12)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "JWT revoked mid-flight by corporate admin; halt forward DAG",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING after auth revocation: %v", err)
	}
}

func TestEdgeCase13_DuplicateOperatorRedriveIdempotency(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 13)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "fail",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationHalt,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "trip circuit breaker",
	})
	if rec.CurrentState != contracts.StateStuck {
		t.Fatalf("expected STUCK: %v", rec.CurrentState)
	}
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOperatorRedrive,
		ExpectedVersion: rec.Version, FencingToken: 2, Reason: "first click re-drive",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("first re-drive failed: %v", err)
	}
	_, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOperatorRedrive,
		ExpectedVersion: 3, FencingToken: 2, Reason: "duplicate accidental second click",
	})
	if err == nil {
		t.Fatalf("expected conflict on duplicate re-drive with stale expectedVersion 3")
	}
}

func TestEdgeCase14_OperatorRedriveWithStaleStateConflict(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 14)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "fail",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationHalt,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "halt",
	})
	_, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOperatorRedrive,
		ExpectedVersion: 1, FencingToken: 2, Reason: "re-drive submitted from stale 20min browser session",
	})
	if err == nil {
		t.Fatalf("expected 409 conflict for stale UI version")
	}
}

func TestEdgeCase15_PoisonPillDAGCycleHalt(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 15)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "poison pill infinite loop detected by Tarjan guard; halt",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected halt & compensation on cycle: %v", err)
	}
}

func TestEdgeCase16_MalformedOrOversizedWebhookRejection(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 16)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventAsyncSuspend,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "awaiting webhook",
	})
	err := contracts.ValidateTransitionStateInput(contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: "", ExpectedVersion: rec.Version, FencingToken: 1, Reason: "",
	})
	if err == nil {
		t.Fatalf("expected closed-schema validation failure for malformed webhook transition")
	}
	curr, _ := svc.GetState(ctx, tenantID, sagaID)
	if curr.CurrentState != contracts.StateWaiting {
		t.Fatalf("malformed webhook altered state machine unexpectedly")
	}
}

func TestEdgeCase17_ProviderAPIContractDriftIndeterminate(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 17)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventIndeterminateError,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "partner changed schema from status:OK to approved:true; marked INDETERMINATE",
	})
	if err != nil || rec.CurrentState != contracts.StateReconciling {
		t.Fatalf("expected RECONCILING on provider drift: %v", err)
	}
}

func TestEdgeCase18_CompensationUnavailableTripToStuck(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 18)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "fail",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationHalt,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "compensation retried 10x with 503; circuit breaker tripped to STUCK",
	})
	if err != nil || rec.CurrentState != contracts.StateStuck {
		t.Fatalf("expected STUCK: %v", err)
	}
}

func TestEdgeCase19_OutboxBacklogStateIsolation(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 19)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventDAGCompleted,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "completed independently of outbox lag",
	})
	if err != nil || rec.CurrentState != contracts.StateCompleted {
		t.Fatalf("expected COMPLETED: %v", err)
	}
}

func TestEdgeCase20_EventOrderingViolationOutOfOrderWebhook(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 20)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	_, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventWebhookApproved,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "out of order webhook received before suspended",
	})
	if err == nil {
		t.Fatalf("expected error applying out-of-order webhook directly to RUNNING state")
	}
}

func TestEdgeCase21_DelayedSuccessAfterTimeoutReconciling(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 21)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventIndeterminateError,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "HTTP 504 timeout",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventProbeSucceeded,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "probe discovered delayed payment succeeded downstream",
	})
	if err != nil || rec.CurrentState != contracts.StateRunning {
		t.Fatalf("expected resume to RUNNING after delayed success probe: %v", err)
	}
}

func TestEdgeCase22_PartialDatabaseCommitAtomicity(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 22)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim",
	})
	audits, _ := svc.GetAuditLogs(ctx, tenantID, sagaID)
	if len(audits) != 1 || audits[0].VersionAfter != rec.Version {
		t.Fatalf("audit log not committed atomically with state transition")
	}
}

func TestEdgeCase23_OperationSucceedsAfterCancellationUndo(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 23)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventClientAbort,
		ExpectedVersion: 1, FencingToken: 1, Reason: "client cancelled order before claim",
	})
	if rec.CurrentState != contracts.StateCancelled {
		t.Fatalf("expected CANCELLED: %v", rec.CurrentState)
	}
	_, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "late settling card charge attempts forward transition",
	})
	if err == nil {
		t.Fatalf("expected rejection when late operation attempts transition on CANCELLED transaction")
	}
}

func TestEdgeCase24_ResourceReservationExpiryBeforePayment(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 24)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 1, Reason: "claim hold",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "reservation TTL expired before payment completed; abort payment and rollback",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected rollback to COMPENSATING: %v", err)
	}
}

func TestEdgeCase25_ManualInterventionOperatorLockConflict(t *testing.T) {
	svc, tenantID, sagaID, ctx := createEdgeCaseFixture(t, 25)
	rec, _ := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: 1, FencingToken: 10, Reason: "SRE operator claims triage lock with token 10",
	})
	_, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 5, Reason: "automated background sweeper with stale token 5 attempts action",
	})
	if err == nil {
		t.Fatalf("expected automated sweeper to be locked out by operator lock fencing token")
	}
}

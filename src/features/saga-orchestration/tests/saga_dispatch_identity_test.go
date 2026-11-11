package tests

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowAdapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	workflowService "distributed-transaction-engine/src/features/workflow-definitions/service"
	"distributed-transaction-engine/src/shared/constants"
	"distributed-transaction-engine/src/shared/utils"
)

type recordedRequest struct {
	Path   string
	Header http.Header
}

type downstreamRecorder struct {
	mu       sync.Mutex
	requests []recordedRequest
	failPath string
}

func (d *downstreamRecorder) handler(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.requests = append(d.requests, recordedRequest{Path: r.URL.Path, Header: r.Header.Clone()})
	failing := r.URL.Path == d.failPath
	d.mu.Unlock()
	if failing {
		http.Error(w, "rejected", http.StatusUnprocessableEntity)
		return
	}
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (d *downstreamRecorder) forPath(path string) []recordedRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	var matched []recordedRequest
	for _, request := range d.requests {
		if request.Path == path {
			matched = append(matched, request)
		}
	}
	return matched
}

func newRecordedDispatcher(t *testing.T, recorder *downstreamRecorder) *sagaAdapters.HttpStepDispatcherAdapter {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(recorder.handler))
	t.Cleanup(server.Close)
	address := server.Listener.Addr().String()
	transport := &http.Transport{DialContext: func(ctx context.Context, network string, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	return sagaAdapters.NewHttpStepDispatcherAdapterWithClient(&http.Client{Transport: transport, Timeout: 5 * time.Second})
}

func httpStep(id string) workflowContracts.StepDefinition {
	return workflowContracts.StepDefinition{
		StepID: id, StepName: id, ActionType: "REST", ActionEndpoint: "http://downstream.test/" + id,
		CompensationAction: "UNDO_" + id, CompensationEndpoint: "http://downstream.test/undo-" + id, TimeoutSeconds: 5,
	}
}

func requireIdentityHeaders(t *testing.T, request recordedRequest, token int64, sagaID string, incarnation string) {
	t.Helper()
	if got := request.Header.Get(constants.HeaderXFencingToken); got != strconv.FormatInt(token, 10) {
		t.Fatalf("%s expected fencing token %d, got %q", request.Path, token, got)
	}
	if got := request.Header.Get(constants.HeaderXSagaID); got != sagaID {
		t.Fatalf("%s expected saga id %s, got %q", request.Path, sagaID, got)
	}
	if got := request.Header.Get(constants.HeaderXEngineIncarnation); got != incarnation {
		t.Fatalf("%s expected incarnation %q, got %q", request.Path, incarnation, got)
	}
	if request.Header.Get("Idempotency-Key") == "" {
		t.Fatalf("%s lost its idempotency key", request.Path)
	}
}

func TestDispatchIdentity_AdapterAttachesHeadersToForwardAndCompensationRequests(t *testing.T) {
	recorder := &downstreamRecorder{}
	dispatcher := newRecordedDispatcher(t, recorder)
	ctx := utils.WithDispatchIdentity(context.Background(), utils.DispatchIdentity{SagaID: "saga-1", FencingToken: 7, IncarnationID: "inc-A"})
	if _, err := dispatcher.DispatchStep(ctx, httpStep("s1"), map[string]interface{}{}, "key-fwd"); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.CompensateStep(ctx, httpStep("s1"), map[string]interface{}{}, map[string]interface{}{}, "key-comp"); err != nil {
		t.Fatal(err)
	}
	requireIdentityHeaders(t, recorder.forPath("/s1")[0], 7, "saga-1", "inc-A")
	requireIdentityHeaders(t, recorder.forPath("/undo-s1")[0], 7, "saga-1", "inc-A")
}

func TestDispatchIdentity_RequestsWithoutAnIdentityCarryNoForgedHeaders(t *testing.T) {
	recorder := &downstreamRecorder{}
	dispatcher := newRecordedDispatcher(t, recorder)
	if _, err := dispatcher.DispatchStep(context.Background(), httpStep("s1"), map[string]interface{}{}, "key"); err != nil {
		t.Fatal(err)
	}
	header := recorder.forPath("/s1")[0].Header
	if header.Get(constants.HeaderXFencingToken) != "" || header.Get(constants.HeaderXEngineIncarnation) != "" {
		t.Fatalf("headers must only be sent when an identity exists: %v", header)
	}
}

type identityFixture struct {
	repo     *sagaAdapters.MemorySagaRepositoryAdapter
	svc      *sagaService.SagaOrchestrationService
	recorder *downstreamRecorder
	tenant   string
	sagaID   string
}

func newIdentityFixture(t *testing.T, tenant string, failPath string) identityFixture {
	t.Helper()
	recorder := &downstreamRecorder{failPath: failPath}
	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	steps := []workflowContracts.StepDefinition{httpStep("s1"), httpStep("s2"), httpStep("s3")}
	steps[1].Dependencies, steps[2].Dependencies = []string{"s1"}, []string{"s2"}
	_, err := workflowService.NewWorkflowDefinitionsService(workflowRepo).RegisterWorkflow(context.Background(), workflowContracts.CreateWorkflowDefinitionInput{
		TenantID: tenant, Name: "identity-flow", Version: 1, Source: "TEST", ExternalID: "ext_identity_wf_" + tenant,
		CorrelationID: "c1000000-0000-0000-0000-000000000301", Steps: steps,
	})
	if err != nil {
		t.Fatal(err)
	}
	repo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	svc := sagaService.NewSagaOrchestrationService(repo, workflowRepo, newRecordedDispatcher(t, recorder)).UseIncarnation("inc-boot-1")
	created, err := svc.StartSaga(context.Background(), sagaContracts.ExecuteSagaInput{
		TenantID: tenant, WorkflowName: "identity-flow", WorkflowVersion: 1, Source: "TEST",
		ExternalID: "ext_identity_order_" + tenant, CorrelationID: "c2000000-0000-0000-0000-000000000301", InitialPayload: map[string]interface{}{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return identityFixture{repo: repo, svc: svc, recorder: recorder, tenant: tenant, sagaID: created.ID}
}

func (f identityFixture) lease(t *testing.T, worker string) *sagaContracts.SagaInstance {
	t.Helper()
	instance, err := f.repo.AcquireLeaseForSaga(context.Background(), f.tenant, f.sagaID, worker, 30*time.Second)
	if err != nil || instance == nil {
		t.Fatalf("lease failed: %v", err)
	}
	return instance
}

func TestDispatchIdentity_EveryStepOfAProcessedSagaCarriesTheLeaseToken(t *testing.T) {
	f := newIdentityFixture(t, "tenant_identity_fwd", "")
	instance := f.lease(t, "worker-1")
	if err := f.svc.ProcessSaga(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/s1", "/s2", "/s3"} {
		requests := f.recorder.forPath(path)
		if len(requests) != 1 {
			t.Fatalf("%s expected one request, got %d", path, len(requests))
		}
		requireIdentityHeaders(t, requests[0], instance.FencingToken, f.sagaID, "inc-boot-1")
	}
}

func TestDispatchIdentity_TokenAdvancesWithEachLeaseSoStaleWorkersAreDistinguishable(t *testing.T) {
	f := newIdentityFixture(t, "tenant_identity_adv", "")
	first := f.lease(t, "worker-1")
	second := f.lease(t, "worker-2")
	if second.FencingToken <= first.FencingToken {
		t.Fatalf("the fencing token must increase with every lease: %d then %d", first.FencingToken, second.FencingToken)
	}
	if err := f.svc.ProcessSaga(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	for _, request := range f.recorder.forPath("/s1") {
		requireIdentityHeaders(t, request, second.FencingToken, f.sagaID, "inc-boot-1")
	}
}

func TestDispatchIdentity_CompensationRequestsCarryTheSameIdentity(t *testing.T) {
	f := newIdentityFixture(t, "tenant_identity_comp", "/s3")
	instance := f.lease(t, "worker-1")
	_ = f.svc.ProcessSaga(context.Background(), instance)
	compensations := f.recorder.forPath("/undo-s2")
	if len(compensations) == 0 {
		t.Fatal("a failure at s3 must compensate s2")
	}
	requireIdentityHeaders(t, compensations[0], instance.FencingToken, f.sagaID, "inc-boot-1")
}

type fencingDownstream struct {
	mu      sync.Mutex
	highest int64
}

func (d *fencingDownstream) handler(w http.ResponseWriter, r *http.Request) {
	token, err := strconv.ParseInt(r.Header.Get(constants.HeaderXFencingToken), 10, 64)
	if err != nil {
		http.Error(w, "fencing token required", http.StatusBadRequest)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if token < d.highest {
		http.Error(w, "stale fencing token", http.StatusConflict)
		return
	}
	d.highest = token
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func TestDispatchIdentity_ADownstreamCanFenceOutAZombieWorkerUsingTheHeaders(t *testing.T) {
	downstream := &fencingDownstream{}
	server := httptest.NewServer(http.HandlerFunc(downstream.handler))
	t.Cleanup(server.Close)
	address := server.Listener.Addr().String()
	dispatcher := sagaAdapters.NewHttpStepDispatcherAdapterWithClient(&http.Client{
		Transport: &http.Transport{DialContext: func(ctx context.Context, network string, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}},
		Timeout: 5 * time.Second,
	})
	send := func(token int64, incarnation string) error {
		ctx := utils.WithDispatchIdentity(context.Background(), utils.DispatchIdentity{SagaID: "saga-1", FencingToken: token, IncarnationID: incarnation})
		_, err := dispatcher.DispatchStep(ctx, httpStep("s1"), map[string]interface{}{}, "key")
		return err
	}
	if err := send(2, "inc-new"); err != nil {
		t.Fatalf("the current worker must be accepted: %v", err)
	}
	if err := send(1, "inc-old"); err == nil {
		t.Fatal("a delayed request from the previous incarnation must be rejected by a fencing aware downstream")
	}
	if err := send(3, "inc-new"); err != nil {
		t.Fatalf("a later token must still be accepted: %v", err)
	}
}

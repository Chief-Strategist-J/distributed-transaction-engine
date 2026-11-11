package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	parallelexecution "distributed-transaction-engine/src/features/parallel-execution"
	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	machines "distributed-transaction-engine/src/features/parallel-execution/domain/machines"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

type scriptedDispatcher struct {
	mu          sync.Mutex
	current     int32
	peak        int32
	dispatched  []string
	cancelled   []string
	compensated []string
	exited      int32
	hold        time.Duration
	blockOn     map[string]bool
	failOn      map[string]error
	cancelErr   error
	sawCause    map[string]error
}

func (d *scriptedDispatcher) DispatchBranch(ctx context.Context, _ string, _ string, _ string, branch contracts.ParallelBranchInstance) error {
	defer atomic.AddInt32(&d.exited, 1)
	now := atomic.AddInt32(&d.current, 1)
	defer atomic.AddInt32(&d.current, -1)
	d.mu.Lock()
	if now > d.peak {
		d.peak = now
	}
	d.dispatched = append(d.dispatched, branch.BranchID)
	failure := d.failOn[branch.BranchID]
	d.mu.Unlock()
	if failure != nil {
		return failure
	}
	if d.blockOn[branch.BranchID] {
		<-ctx.Done()
		d.mu.Lock()
		if d.sawCause == nil {
			d.sawCause = map[string]error{}
		}
		d.sawCause[branch.BranchID] = context.Cause(ctx)
		d.mu.Unlock()
		return ctx.Err()
	}
	time.Sleep(d.hold)
	return nil
}

func (d *scriptedDispatcher) CancelBranch(_ context.Context, _ string, _ string, _ string, branchID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cancelled = append(d.cancelled, branchID)
	return d.cancelErr
}

func (d *scriptedDispatcher) CompensateBranch(_ context.Context, _ string, _ string, _ string, branchID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.compensated = append(d.compensated, branchID)
	return nil
}

func groupInput(policy contracts.JoinPolicy, count int) *contracts.StartParallelGroupInput {
	branches := make([]contracts.BranchDefinitionInput, 0, count)
	for index := 0; index < count; index++ {
		branches = append(branches, contracts.BranchDefinitionInput{
			BranchID: fmt.Sprintf("b%03d", index), Name: "branch", Role: contracts.RoleRequired, EffectType: contracts.EffectCompensable,
			Payload: map[string]interface{}{"n": index},
		})
	}
	return &contracts.StartParallelGroupInput{
		ID: uuid.NewString(), PublicID: "par_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:26], TenantID: "tenant_par",
		SagaID: uuid.NewString(), JoinPolicy: policy, QuorumCount: 1, Branches: branches,
	}
}

func newParallelService(dispatcher *scriptedDispatcher, limit int) (*parallelexecutionService, parallelexecution.RepositoryPort) {
	repo := parallelexecution.NewMemoryAdapter()
	svc := parallelexecution.NewService(parallelexecution.ServiceDeps{
		Repo: repo, Dispatcher: dispatcher, Machine: machines.NewParallelExecutionMachine(),
		Tracer: observability.NewTracer("parallel-scenario"), MaxConcurrentBranches: limit,
	})
	return svc, repo
}

type parallelexecutionService = parallelexecution.Service

func TestScenario_FanOutNeverExceedsTheConcurrencyLimitAndQueuesTheRest(t *testing.T) {
	cases := []struct {
		name  string
		limit int
		want  int32
	}{{"explicit_limit_of_8", 8, 8}, {"default_is_32", 0, 32}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dispatcher := &scriptedDispatcher{hold: 20 * time.Millisecond}
			svc, _ := newParallelService(dispatcher, tc.limit)
			if _, err := svc.StartParallelGroup(context.Background(), groupInput(contracts.PolicyAllSettled, 100)); err != nil {
				t.Fatal(err)
			}
			if len(dispatcher.dispatched) != 100 {
				t.Fatalf("every branch must eventually be dispatched, got %d", len(dispatcher.dispatched))
			}
			if dispatcher.peak > tc.want {
				t.Fatalf("concurrency peaked at %d, above the limit %d", dispatcher.peak, tc.want)
			}
			if dispatcher.peak < tc.want {
				t.Fatalf("the pool should actually run %d branches in parallel, peak was %d", tc.want, dispatcher.peak)
			}
		})
	}
}

func TestScenario_RequiredBranchDispatchFailureCancelsSiblingsAndWaitsForThem(t *testing.T) {
	dispatcher := &scriptedDispatcher{
		blockOn: map[string]bool{"b001": true, "b002": true},
		failOn:  map[string]error{"b000": fmt.Errorf("downstream refused")},
	}
	svc, repo := newParallelService(dispatcher, 3)
	input := groupInput(contracts.PolicyAllRequired, 12)
	group := startWithin(t, svc, input, 5*time.Second)
	if exited := atomic.LoadInt32(&dispatcher.exited); int(exited) != len(dispatcher.dispatched) {
		t.Fatalf("StartParallelGroup returned while %d dispatches were still running", len(dispatcher.dispatched)-int(exited))
	}
	stored, err := repo.GetGroup(context.Background(), input.TenantID, group.ID)
	if err != nil || stored.CurrentState != contracts.GroupStateJoinFailed {
		t.Fatalf("a failed required branch must fail the group, got %v err=%v", stored, err)
	}
	for _, blocked := range []string{"b001", "b002"} {
		if dispatcher.sawCause[blocked] == nil || !strings.Contains(dispatcher.sawCause[blocked].Error(), "join decision") {
			t.Fatalf("sibling %s must be cancelled with the join decision as cause, got %v", blocked, dispatcher.sawCause[blocked])
		}
	}
	if len(dispatcher.dispatched) >= 12 {
		t.Fatalf("branches queued behind the failure must not be dispatched, all %d were", len(dispatcher.dispatched))
	}
	if len(dispatcher.cancelled) == 0 {
		t.Fatal("the join decision must also emit cancel commands for in-flight siblings")
	}
}

func TestScenario_OptionalFailureUnderFirstSuccessDoesNotCancelOtherBranches(t *testing.T) {
	dispatcher := &scriptedDispatcher{hold: 10 * time.Millisecond, failOn: map[string]error{"b000": fmt.Errorf("one replica is down")}}
	svc, repo := newParallelService(dispatcher, 4)
	input := groupInput(contracts.PolicyFirstSuccess, 6)
	group, err := svc.StartParallelGroup(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.dispatched) != 6 {
		t.Fatalf("one failed alternative must not stop the others, dispatched %d", len(dispatcher.dispatched))
	}
	stored, _ := repo.GetGroup(context.Background(), input.TenantID, group.ID)
	if stored.CurrentState != contracts.GroupStateFanoutActive {
		t.Fatalf("the group must stay active until a branch succeeds, got %s", stored.CurrentState)
	}
}

func TestScenario_FailedCancelCommandsAreReportedNotSwallowed(t *testing.T) {
	dispatcher := &scriptedDispatcher{cancelErr: fmt.Errorf("cancel endpoint unreachable")}
	svc, repo := newParallelService(dispatcher, 4)
	input := groupInput(contracts.PolicyAllRequired, 4)
	group, err := svc.StartParallelGroup(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	err = svc.HandleBranchCompletion(context.Background(), input.TenantID, group.ID, "b000", false, nil, "boom")
	if err == nil || !strings.Contains(err.Error(), "group command(s) failed") || !strings.Contains(err.Error(), "cancel endpoint unreachable") {
		t.Fatalf("a failed cancel must be surfaced, got %v", err)
	}
	stored, _ := repo.GetGroup(context.Background(), input.TenantID, group.ID)
	if stored.CurrentState != contracts.GroupStateJoinFailed {
		t.Fatalf("the join decision must still be persisted, got %s", stored.CurrentState)
	}
}

func TestScenario_ConcurrentDispatchFailuresSettleTheGroupExactlyOnce(t *testing.T) {
	failures := map[string]error{}
	for index := 0; index < 20; index++ {
		failures[fmt.Sprintf("b%03d", index)] = fmt.Errorf("refused")
	}
	dispatcher := &scriptedDispatcher{failOn: failures}
	svc, repo := newParallelService(dispatcher, 20)
	input := groupInput(contracts.PolicyAllRequired, 20)
	group, err := svc.StartParallelGroup(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	stored, _ := repo.GetGroup(context.Background(), input.TenantID, group.ID)
	if stored.CurrentState != contracts.GroupStateJoinFailed {
		t.Fatalf("expected JOIN_FAILED, got %s", stored.CurrentState)
	}
}

func startWithin(t *testing.T, svc *parallelexecutionService, input *contracts.StartParallelGroupInput, limit time.Duration) *contracts.ParallelGroupInstance {
	t.Helper()
	type outcome struct {
		group *contracts.ParallelGroupInstance
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		group, err := svc.StartParallelGroup(context.Background(), input)
		done <- outcome{group, err}
	}()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.group
	case <-time.After(limit):
		t.Fatalf("StartParallelGroup did not return within %v: siblings of a failed branch were not cancelled", limit)
		return nil
	}
}

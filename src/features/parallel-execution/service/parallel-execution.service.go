package service

import (
	"context"
	stderrors "errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	machines "distributed-transaction-engine/src/features/parallel-execution/domain/machines"
	ports "distributed-transaction-engine/src/features/parallel-execution/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

const DefaultMaxConcurrentBranches = 32

type ParallelExecutionServiceDeps struct {
	Repo                  ports.ParallelExecutionRepositoryPort
	Dispatcher            ports.ParallelDispatcherPort
	Machine               *machines.ParallelExecutionMachine
	Tracer                *observability.DefaultTracer
	MaxConcurrentBranches int
}

type ParallelExecutionService struct {
	repo          ports.ParallelExecutionRepositoryPort
	dispatcher    ports.ParallelDispatcherPort
	machine       *machines.ParallelExecutionMachine
	tracer        *observability.DefaultTracer
	maxConcurrent int
}

func NewParallelExecutionService(deps ParallelExecutionServiceDeps) *ParallelExecutionService {
	maxConcurrent := deps.MaxConcurrentBranches
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentBranches
	}
	return &ParallelExecutionService{
		repo:          deps.Repo,
		dispatcher:    deps.Dispatcher,
		machine:       deps.Machine,
		tracer:        deps.Tracer,
		maxConcurrent: maxConcurrent,
	}
}

func (s *ParallelExecutionService) StartParallelGroup(ctx context.Context, input *contracts.StartParallelGroupInput) (spanResult0 *contracts.ParallelGroupInstance, spanErr error) {
	_, span := s.tracer.StartSpan(ctx, "StartParallelGroup")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateStartParallelGroupInput(input); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	group := &contracts.ParallelGroupInstance{
		ID:            input.ID,
		PublicID:      input.PublicID,
		TenantID:      input.TenantID,
		SagaID:        input.SagaID,
		JoinPolicy:    input.JoinPolicy,
		QuorumCount:   input.QuorumCount,
		CurrentState:  contracts.GroupStateFanoutActive,
		TotalBranches: len(input.Branches),
		Version:       1,
		CorrelationID: uuid.New().String(),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	branches := make([]contracts.ParallelBranchInstance, len(input.Branches))
	for i, b := range input.Branches {
		bID := uuid.New().String()
		uStr := uuid.New().String()
		cleaned := ""
		for _, ch := range uStr {
			if ch != '-' {
				cleaned += string(ch)
			}
		}
		if len(cleaned) > 26 {
			cleaned = cleaned[:26]
		}
		branches[i] = contracts.ParallelBranchInstance{
			ID:             bID,
			PublicID:       fmt.Sprintf("pbr_%s", cleaned),
			TenantID:       input.TenantID,
			GroupID:        input.ID,
			SagaID:         input.SagaID,
			BranchID:       b.BranchID,
			BranchName:     b.Name,
			Role:           b.Role,
			EffectType:     b.EffectType,
			CurrentState:   contracts.BranchStateDispatched,
			IdempotencyKey: fmt.Sprintf("%s:%s:%s:1", input.SagaID, input.ID, b.BranchID),
			BranchInput:    b.Payload,
			BranchOutput:   make(map[string]interface{}),
			CorrelationID:  group.CorrelationID,
			Version:        1,
			DispatchedAt:   &now,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}

	if err := s.repo.CreateGroupAtomic(ctx, group, branches); err != nil {
		return nil, err
	}

	s.dispatchBranches(ctx, group, branches)

	group.Branches = branches
	return group, nil
}

var errGroupDecided = stderrors.New("parallel group reached its join decision")

func (s *ParallelExecutionService) dispatchBranches(ctx context.Context, group *contracts.ParallelGroupInstance, branches []contracts.ParallelBranchInstance) {
	dispatchCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	slots := make(chan struct{}, s.maxConcurrent)
	var running sync.WaitGroup
	for _, branch := range branches {
		select {
		case slots <- struct{}{}:
		case <-dispatchCtx.Done():
			running.Wait()
			return
		}
		running.Add(1)
		go func(candidate contracts.ParallelBranchInstance) {
			defer running.Done()
			defer func() { <-slots }()
			if err := s.dispatcher.DispatchBranch(dispatchCtx, group.TenantID, group.SagaID, group.ID, candidate); err != nil {
				s.recordDispatchFailure(ctx, dispatchCtx, cancel, group, candidate, err)
			}
		}(branch)
	}
	running.Wait()
}

func (s *ParallelExecutionService) recordDispatchFailure(
	ctx context.Context,
	dispatchCtx context.Context,
	cancel context.CancelCauseFunc,
	group *contracts.ParallelGroupInstance,
	branch contracts.ParallelBranchInstance,
	dispatchErr error,
) {
	if dispatchCtx.Err() != nil && stderrors.Is(dispatchErr, context.Canceled) {
		return
	}
	observability.GetLogger().Warn(ctx, "parallel branch dispatch failed",
		observability.String("group_id", group.ID), observability.String("branch_id", branch.BranchID), observability.String("error", dispatchErr.Error()))
	for attempt := 0; attempt < maxCompletionAttempts; attempt++ {
		err := s.HandleBranchCompletion(ctx, group.TenantID, group.ID, branch.BranchID, false, nil, "dispatch failed: "+dispatchErr.Error())
		if err == nil {
			break
		}
		if !isVersionConflict(err) {
			observability.GetLogger().Error(ctx, "recording parallel branch failure failed", observability.String("error", err.Error()))
			return
		}
	}
	if current, err := s.repo.GetGroup(ctx, group.TenantID, group.ID); err == nil && current.CurrentState != contracts.GroupStateFanoutActive {
		cancel(errGroupDecided)
	}
}

const maxCompletionAttempts = 3

func isVersionConflict(err error) bool {
	var appErr *errors.ApplicationError
	return stderrors.As(err, &appErr) && appErr.StatusCode == 409
}

func (s *ParallelExecutionService) HandleBranchCompletion(
	ctx context.Context,
	tenantID string,
	groupID string,
	branchID string,
	isSuccess bool,
	output map[string]interface{},
	errMsg string,
) (spanErr error) {
	_, span := s.tracer.StartSpan(ctx, "HandleBranchCompletion")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	branchState := contracts.BranchStateSucceeded
	if !isSuccess {
		branchState = contracts.BranchStateRejected
	}

	if err := s.repo.UpdateBranchOutcome(ctx, tenantID, groupID, branchID, branchState, output, errMsg); err != nil {
		return err
	}

	group, err := s.repo.GetGroup(ctx, tenantID, groupID)
	if err != nil {
		return err
	}

	if group.CurrentState != contracts.GroupStateFanoutActive {
		return nil
	}

	dec := s.machine.EvaluateGroupJoin(group.JoinPolicy, group.QuorumCount, group.Branches)
	if dec.GroupState == contracts.GroupStateFanoutActive {
		return nil
	}

	succeededCount := 0
	failedCount := 0
	for _, b := range group.Branches {
		if b.CurrentState == contracts.BranchStateSucceeded {
			succeededCount++
		} else if b.CurrentState == contracts.BranchStateRejected {
			failedCount++
		}
	}

	if err := s.repo.UpdateGroupCAS(ctx, tenantID, groupID, group.Version, dec.GroupState, succeededCount, failedCount); err != nil {
		return err
	}

	return s.runGroupCommands(ctx, tenantID, group.SagaID, groupID, dec.Commands)
}

func (s *ParallelExecutionService) runGroupCommands(ctx context.Context, tenantID string, sagaID string, groupID string, commands []machines.ParallelCommand) error {
	slots := make(chan struct{}, s.maxConcurrent)
	var running sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	for _, cmd := range commands {
		if cmd.Type != machines.CommandCancelBranch && cmd.Type != machines.CommandCompensateBranch {
			continue
		}
		slots <- struct{}{}
		running.Add(1)
		go func(command machines.ParallelCommand) {
			defer running.Done()
			defer func() { <-slots }()
			if err := s.executeCommand(ctx, tenantID, sagaID, groupID, command); err != nil {
				mu.Lock()
				failures = append(failures, fmt.Sprintf("%s %s: %v", command.Type, command.BranchID, err))
				mu.Unlock()
			}
		}(cmd)
	}
	running.Wait()
	if len(failures) > 0 {
		sort.Strings(failures)
		return errors.NewInternalError(fmt.Sprintf("%d group command(s) failed: %s", len(failures), strings.Join(failures, "; ")))
	}
	return nil
}

func (s *ParallelExecutionService) executeCommand(ctx context.Context, tenantID string, sagaID string, groupID string, command machines.ParallelCommand) error {
	if command.Type == machines.CommandCancelBranch {
		return s.dispatcher.CancelBranch(ctx, tenantID, sagaID, groupID, command.BranchID)
	}
	return s.dispatcher.CompensateBranch(ctx, tenantID, sagaID, groupID, command.BranchID)
}

func (s *ParallelExecutionService) GetGroup(ctx context.Context, tenantID string, groupID string) (spanResult0 *contracts.ParallelGroupInstance, spanErr error) {
	_, span := s.tracer.StartSpan(ctx, "GetGroup")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if tenantID == "" || groupID == "" {
		return nil, errors.NewValidationError("tenantId and groupId cannot be empty")
	}
	return s.repo.GetGroup(ctx, tenantID, groupID)
}

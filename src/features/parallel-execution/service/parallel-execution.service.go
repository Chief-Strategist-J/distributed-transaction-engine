package service

import (
	"context"
	"fmt"
	"time"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	machines "distributed-transaction-engine/src/features/parallel-execution/domain/machines"
	ports "distributed-transaction-engine/src/features/parallel-execution/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type ParallelExecutionServiceDeps struct {
	Repo       ports.ParallelExecutionRepositoryPort
	Dispatcher ports.ParallelDispatcherPort
	Machine    *machines.ParallelExecutionMachine
	Tracer     *observability.DefaultTracer
}

type ParallelExecutionService struct {
	repo       ports.ParallelExecutionRepositoryPort
	dispatcher ports.ParallelDispatcherPort
	machine    *machines.ParallelExecutionMachine
	tracer     *observability.DefaultTracer
}

func NewParallelExecutionService(deps ParallelExecutionServiceDeps) *ParallelExecutionService {
	return &ParallelExecutionService{
		repo:       deps.Repo,
		dispatcher: deps.Dispatcher,
		machine:    deps.Machine,
		tracer:     deps.Tracer,
	}
}

func (s *ParallelExecutionService) StartParallelGroup(ctx context.Context, input *contracts.StartParallelGroupInput) (*contracts.ParallelGroupInstance, error) {
	_, span := s.tracer.StartSpan(ctx, "StartParallelGroup")
	defer span.End()

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

	for _, b := range branches {
		_ = s.dispatcher.DispatchBranch(ctx, group.TenantID, group.SagaID, group.ID, b)
	}

	group.Branches = branches
	return group, nil
}

func (s *ParallelExecutionService) HandleBranchCompletion(
	ctx context.Context,
	tenantID string,
	groupID string,
	branchID string,
	isSuccess bool,
	output map[string]interface{},
	errMsg string,
) error {
	_, span := s.tracer.StartSpan(ctx, "HandleBranchCompletion")
	defer span.End()

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

	for _, cmd := range dec.Commands {
		switch cmd.Type {
		case machines.CommandCancelBranch:
			_ = s.dispatcher.CancelBranch(ctx, tenantID, group.SagaID, groupID, cmd.BranchID)
		case machines.CommandCompensateBranch:
			_ = s.dispatcher.CompensateBranch(ctx, tenantID, group.SagaID, groupID, cmd.BranchID)
		}
	}

	return nil
}

func (s *ParallelExecutionService) GetGroup(ctx context.Context, tenantID string, groupID string) (*contracts.ParallelGroupInstance, error) {
	_, span := s.tracer.StartSpan(ctx, "GetGroup")
	defer span.End()

	if tenantID == "" || groupID == "" {
		return nil, errors.NewValidationError("tenantId and groupId cannot be empty")
	}
	return s.repo.GetGroup(ctx, tenantID, groupID)
}

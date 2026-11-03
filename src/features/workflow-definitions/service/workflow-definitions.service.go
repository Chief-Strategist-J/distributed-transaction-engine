package service

import (
	"context"
	"fmt"
	"time"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	rules "distributed-transaction-engine/src/features/workflow-definitions/domain/rules"
	ports "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type WorkflowDefinitionsService struct {
	repo   ports.WorkflowDefinitionsRepositoryPort
	logger *observability.LokiLogger
	tracer *observability.DefaultTracer
}

func NewWorkflowDefinitionsService(repo ports.WorkflowDefinitionsRepositoryPort) *WorkflowDefinitionsService {
	return &WorkflowDefinitionsService{
		repo:   repo,
		logger: observability.GetLogger(),
		tracer: observability.NewTracer("workflow-definitions-service"),
	}
}

func (s *WorkflowDefinitionsService) RegisterWorkflow(
	ctx context.Context,
	input contracts.CreateWorkflowDefinitionInput,
) (*contracts.WorkflowDefinitionInstance, error) {
	ctx, span := s.tracer.StartSpan(ctx, "workflow_service.register")
	defer span.End()

	if err := contracts.ValidateCreateWorkflowDefinitionInput(input); err != nil {
		observability.RecordError(span, err)
		s.logger.Warn(ctx, "workflow input validation failed", observability.String("error", err.Error()))
		return nil, err
	}

	dagResult := rules.ValidateDAGTopology(input.Steps)
	if !dagResult.IsValid {
		observability.RecordError(span, dagResult.Error)
		s.logger.Warn(ctx, "workflow DAG topology validation failed", observability.String("error", dagResult.Error.Error()))
		return nil, dagResult.Error
	}

	specHash, err := contracts.ComputeSpecHash(input.Name, input.Steps)
	if err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "failed to compute workflow hash", observability.String("error", err.Error()))
		return nil, errors.NewInternalError(fmt.Sprintf("failed to compute workflow hash: %v", err))
	}

	existingByHash, _ := s.repo.GetBySpecHash(ctx, input.TenantID, input.Name, specHash)
	if existingByHash != nil {
		conflictErr := errors.NewConflictError("identical workflow spec is already registered")
		observability.RecordError(span, conflictErr)
		s.logger.Warn(ctx, "identical workflow spec already registered", observability.String("spec_hash", specHash))
		return nil, conflictErr
	}

	existingByNameVer, _ := s.repo.GetByNameAndVersion(ctx, input.TenantID, input.Name, input.Version)
	if existingByNameVer != nil {
		conflictErr := errors.NewConflictError(fmt.Sprintf("workflow %s version %d already exists", input.Name, input.Version))
		observability.RecordError(span, conflictErr)
		s.logger.Warn(ctx, "workflow name and version conflict", observability.String("name", input.Name), observability.Int64("version", input.Version))
		return nil, conflictErr
	}

	u := uuid.New()
	id := u.String()
	publicID := fmt.Sprintf("wfd_%s%s", stringsReplaceAll(u.String(), "-", ""), "0000000000000000000000")[:30]
	now := time.Now().UTC().Format(time.RFC3339)

	instance := &contracts.WorkflowDefinitionInstance{
		ID:            id,
		PublicID:      publicID,
		TenantID:      input.TenantID,
		Name:          input.Name,
		Version:       input.Version,
		Source:        input.Source,
		ExternalID:    input.ExternalID,
		CorrelationID: input.CorrelationID,
		SpecHash:      specHash,
		Status:        "ACTIVE",
		TotalSteps:    len(input.Steps),
		Steps:         input.Steps,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.repo.Save(ctx, instance); err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "failed to persist workflow definition", observability.String("error", err.Error()))
		return nil, err
	}

	s.logger.Info(ctx, "workflow definition registered successfully",
		observability.String("workflow_id", instance.ID),
		observability.String("name", instance.Name),
		observability.Int64("version", instance.Version),
		observability.String("tenant_id", instance.TenantID),
	)

	return instance, nil
}

func (s *WorkflowDefinitionsService) GetWorkflow(ctx context.Context, tenantID string, id string) (*contracts.WorkflowDefinitionInstance, error) {
	ctx, span := s.tracer.StartSpan(ctx, "workflow_service.get")
	defer span.End()

	if tenantID == "" || id == "" {
		err := errors.NewValidationError("tenantId and id are required")
		observability.RecordError(span, err)
		s.logger.Warn(ctx, "missing required parameters for get workflow", observability.String("tenant_id", tenantID), observability.String("id", id))
		return nil, err
	}

	res, err := s.repo.GetByID(ctx, tenantID, id)
	if err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "error querying workflow definition by id", observability.String("id", id), observability.String("error", err.Error()))
		return nil, err
	}
	return res, nil
}

func (s *WorkflowDefinitionsService) GetWorkflowByNameAndVersion(ctx context.Context, tenantID string, name string, version int64) (*contracts.WorkflowDefinitionInstance, error) {
	ctx, span := s.tracer.StartSpan(ctx, "workflow_service.get_by_name_and_version")
	defer span.End()

	if tenantID == "" || name == "" || version < 1 {
		err := errors.NewValidationError("tenantId, name, and valid version are required")
		observability.RecordError(span, err)
		s.logger.Warn(ctx, "missing parameters for get workflow by name and version")
		return nil, err
	}

	res, err := s.repo.GetByNameAndVersion(ctx, tenantID, name, version)
	if err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "error querying workflow by name and version", observability.String("name", name), observability.Int64("version", version), observability.String("error", err.Error()))
		return nil, err
	}
	return res, nil
}

func (s *WorkflowDefinitionsService) ListWorkflows(ctx context.Context, tenantID string, limit int, offset int) ([]*contracts.WorkflowDefinitionInstance, error) {
	ctx, span := s.tracer.StartSpan(ctx, "workflow_service.list")
	defer span.End()

	if tenantID == "" {
		err := errors.NewValidationError("tenantId is required")
		observability.RecordError(span, err)
		s.logger.Warn(ctx, "missing tenant_id for list workflows")
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	res, err := s.repo.ListByTenant(ctx, tenantID, limit, offset)
	if err != nil {
		observability.RecordError(span, err)
		s.logger.Error(ctx, "error listing workflows", observability.String("tenant_id", tenantID), observability.String("error", err.Error()))
		return nil, err
	}
	return res, nil
}

func stringsReplaceAll(s, old, new string) string {
	res := ""
	for i := 0; i < len(s); i++ {
		if string(s[i]) != old {
			res += string(s[i])
		} else {
			res += new
		}
	}
	return res
}


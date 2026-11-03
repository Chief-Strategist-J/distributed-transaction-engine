package service

import (
	"context"
	"fmt"
	"time"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	rules "distributed-transaction-engine/src/features/workflow-definitions/domain/rules"
	ports "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type WorkflowDefinitionsService struct {
	repo ports.WorkflowDefinitionsRepositoryPort
}

func NewWorkflowDefinitionsService(repo ports.WorkflowDefinitionsRepositoryPort) *WorkflowDefinitionsService {
	return &WorkflowDefinitionsService{
		repo: repo,
	}
}

func (s *WorkflowDefinitionsService) RegisterWorkflow(
	ctx context.Context,
	input contracts.CreateWorkflowDefinitionInput,
) (*contracts.WorkflowDefinitionInstance, error) {
	if err := contracts.ValidateCreateWorkflowDefinitionInput(input); err != nil {
		return nil, err
	}

	dagResult := rules.ValidateDAGTopology(input.Steps)
	if !dagResult.IsValid {
		return nil, dagResult.Error
	}

	specHash, err := contracts.ComputeSpecHash(input.Name, input.Steps)
	if err != nil {
		return nil, errors.NewInternalError(fmt.Sprintf("failed to compute workflow hash: %v", err))
	}

	existingByHash, _ := s.repo.GetBySpecHash(ctx, input.TenantID, input.Name, specHash)
	if existingByHash != nil {
		return nil, errors.NewConflictError("identical workflow spec is already registered")
	}

	existingByNameVer, _ := s.repo.GetByNameAndVersion(ctx, input.TenantID, input.Name, input.Version)
	if existingByNameVer != nil {
		return nil, errors.NewConflictError(fmt.Sprintf("workflow %s version %d already exists", input.Name, input.Version))
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
		return nil, err
	}

	return instance, nil
}

func (s *WorkflowDefinitionsService) GetWorkflow(ctx context.Context, tenantID string, id string) (*contracts.WorkflowDefinitionInstance, error) {
	if tenantID == "" || id == "" {
		return nil, errors.NewValidationError("tenantId and id are required")
	}
	return s.repo.GetByID(ctx, tenantID, id)
}

func (s *WorkflowDefinitionsService) GetWorkflowByNameAndVersion(ctx context.Context, tenantID string, name string, version int64) (*contracts.WorkflowDefinitionInstance, error) {
	if tenantID == "" || name == "" || version < 1 {
		return nil, errors.NewValidationError("tenantId, name, and valid version are required")
	}
	return s.repo.GetByNameAndVersion(ctx, tenantID, name, version)
}

func (s *WorkflowDefinitionsService) ListWorkflows(ctx context.Context, tenantID string, limit int, offset int) ([]*contracts.WorkflowDefinitionInstance, error) {
	if tenantID == "" {
		return nil, errors.NewValidationError("tenantId is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.ListByTenant(ctx, tenantID, limit, offset)
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


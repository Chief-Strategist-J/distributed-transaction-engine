package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"strings"
)

func ValidateRegisterBundle(cmd *RegisterBundleCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command is nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(cmd.WorkflowName) == "" {
		return appErrors.NewValidationError("workflow_name is required")
	}
	if strings.TrimSpace(cmd.VersionTag) == "" {
		return appErrors.NewValidationError("version_tag is required")
	}
	if cmd.BundlePayload == nil || len(cmd.BundlePayload) == 0 {
		return appErrors.NewValidationError("bundle_payload cannot be empty")
	}
	return nil
}

func ValidateProposePromotion(cmd *ProposePromotionCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command is nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if cmd.BundleID == uuid.Nil {
		return appErrors.NewValidationError("bundle_id is required")
	}
	if strings.TrimSpace(cmd.ProposerID) == "" {
		return appErrors.NewValidationError("proposer_id is required")
	}
	if !isValidPromotionAction(cmd.Action) {
		return appErrors.NewValidationError("invalid promotion action")
	}
	return nil
}

func ValidateDecidePromotion(cmd *DecidePromotionCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command is nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if cmd.PromotionID == uuid.Nil {
		return appErrors.NewValidationError("promotion_id is required")
	}
	if strings.TrimSpace(cmd.ApproverID) == "" {
		return appErrors.NewValidationError("approver_id is required")
	}
	return nil
}

func ValidateResolveVersion(query *ResolveVersionQuery) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if query == nil {
		return appErrors.NewValidationError("query is nil")
	}
	if strings.TrimSpace(query.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(query.WorkflowName) == "" {
		return appErrors.NewValidationError("workflow_name is required")
	}
	if strings.TrimSpace(query.EntityID) == "" {
		return appErrors.NewValidationError("entity_id is required")
	}
	return nil
}

func ValidateMigrateSaga(cmd *MigrateSagaCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command is nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if cmd.SagaID == uuid.Nil {
		return appErrors.NewValidationError("saga_id is required")
	}
	if strings.TrimSpace(cmd.TargetVersionTag) == "" {
		return appErrors.NewValidationError("target_version_tag is required")
	}
	if strings.TrimSpace(cmd.SafePointStepID) == "" {
		return appErrors.NewValidationError("safe_point_step_id is required")
	}
	return nil
}

func isValidPromotionAction(a PromotionAction) bool {
	switch a {
	case PromotionActionPublish, PromotionActionPromote, PromotionActionDeprecate, PromotionActionBlock, PromotionActionArchive:
		return true
	default:
		return false
	}
}

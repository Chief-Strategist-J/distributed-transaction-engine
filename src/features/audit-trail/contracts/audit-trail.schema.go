package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"strings"
)

func ValidateCaptureOutboxEvent(cmd *CaptureOutboxEventCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if strings.TrimSpace(cmd.Source) == "" {
		return appErrors.NewValidationError("source is required")
	}
	if strings.TrimSpace(cmd.ExternalID) == "" {
		return appErrors.NewValidationError("external_id is required")
	}
	if cmd.CorrelationID == uuid.Nil {
		return appErrors.NewValidationError("correlation_id is required")
	}
	if strings.TrimSpace(cmd.AuditID) == "" {
		return appErrors.NewValidationError("audit_id is required")
	}
	if strings.TrimSpace(cmd.Action) == "" {
		return appErrors.NewValidationError("action is required")
	}
	if cmd.OccurredAt.IsZero() {
		return appErrors.NewValidationError("occurred_at is required")
	}
	return nil
}

func ValidateProcessChainerBatch(cmd *ProcessChainerBatchCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if cmd.SegmentID == uuid.Nil {
		return appErrors.NewValidationError("segment_id is required")
	}
	if strings.TrimSpace(cmd.KeyID) == "" {
		return appErrors.NewValidationError("key_id is required")
	}
	if len(cmd.SecretKey) == 0 {
		return appErrors.NewValidationError("secret_key is required")
	}
	if cmd.BatchSize <= 0 {
		cmd.BatchSize = 100
	}
	if cmd.BatchSize > 1000 {
		cmd.BatchSize = 1000
	}
	return nil
}

func ValidateCreateCheckpoint(cmd *CreateCheckpointCommand) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cmd == nil {
		return appErrors.NewValidationError("command cannot be nil")
	}
	if strings.TrimSpace(cmd.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if cmd.SegmentID == uuid.Nil {
		return appErrors.NewValidationError("segment_id is required")
	}
	if strings.TrimSpace(cmd.KeyID) == "" {
		return appErrors.NewValidationError("key_id is required")
	}
	if len(cmd.SigningKey) == 0 {
		return appErrors.NewValidationError("signing_key is required")
	}
	return nil
}

func ValidateVerifyChain(query *VerifyChainQuery) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if query == nil {
		return appErrors.NewValidationError("query cannot be nil")
	}
	if strings.TrimSpace(query.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if query.SegmentID == uuid.Nil {
		return appErrors.NewValidationError("segment_id is required")
	}
	if strings.TrimSpace(query.KeyID) == "" {
		return appErrors.NewValidationError("key_id is required")
	}
	if len(query.SecretKey) == 0 {
		return appErrors.NewValidationError("secret_key is required")
	}
	if strings.TrimSpace(query.VerifiedBy) == "" {
		return appErrors.NewValidationError("verified_by is required")
	}
	return nil
}

func ValidateListAuditEvents(query *ListAuditEventsQuery) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if query == nil {
		return appErrors.NewValidationError("query cannot be nil")
	}
	if strings.TrimSpace(query.TenantID) == "" {
		return appErrors.NewValidationError("tenant_id is required")
	}
	if query.Limit <= 0 {
		query.Limit = 50
	}
	if query.Limit > 500 {
		query.Limit = 500
	}
	return nil
}

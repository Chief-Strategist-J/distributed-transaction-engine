package contracts

import (
	"bytes"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"strings"
)

func ValidateConnectStreamRequest(body []byte) (spanResult0 *ConnectStreamRequest, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if len(body) == 0 {
		return nil, appErrors.NewValidationError("request body cannot be empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var req ConnectStreamRequest
	if err := decoder.Decode(&req); err != nil {
		return nil, appErrors.NewValidationError("invalid request format: " + err.Error())
	}

	req.PrincipalID = strings.TrimSpace(req.PrincipalID)
	if req.PrincipalID == "" {
		return nil, appErrors.NewValidationError("principal_id is mandatory and cannot be empty")
	}

	switch req.ScopeType {
	case ScopeTenantAdmin, ScopeSupport, ScopeOperator:
	default:
		return nil, appErrors.NewValidationError("invalid scope_type: must be TENANT_ADMIN, SUPPORT, or OPERATOR")
	}

	if req.ScopeType == ScopeSupport {
		if req.ImpersonationTicket == nil || strings.TrimSpace(*req.ImpersonationTicket) == "" {
			return nil, appErrors.NewValidationError("impersonation_ticket is required for SUPPORT scope")
		}
		trimmed := strings.TrimSpace(*req.ImpersonationTicket)
		req.ImpersonationTicket = &trimmed
	}

	return &req, nil
}

func ValidatePreviewActionRequest(body []byte) (spanResult0 *PreviewActionRequest, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if len(body) == 0 {
		return nil, appErrors.NewValidationError("request body cannot be empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var req PreviewActionRequest
	if err := decoder.Decode(&req); err != nil {
		return nil, appErrors.NewValidationError("invalid request format: " + err.Error())
	}

	req.SagaID = strings.TrimSpace(req.SagaID)
	if req.SagaID == "" {
		return nil, appErrors.NewValidationError("saga_id is mandatory")
	}

	req.OperatorID = strings.TrimSpace(req.OperatorID)
	if req.OperatorID == "" {
		return nil, appErrors.NewValidationError("operator_id is mandatory")
	}

	if req.CurrentSagaVersion <= 0 {
		return nil, appErrors.NewValidationError("current_saga_version must be strictly positive")
	}

	switch req.ActionType {
	case ActionPause, ActionResume, ActionRetryStep, ActionForceCompensate, ActionMarkCompleted, ActionSkipStep:
	default:
		return nil, appErrors.NewValidationError("unsupported action_type")
	}

	return &req, nil
}

func ValidateExecuteActionRequest(body []byte) (spanResult0 *ExecuteActionRequest, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if len(body) == 0 {
		return nil, appErrors.NewValidationError("request body cannot be empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var req ExecuteActionRequest
	if err := decoder.Decode(&req); err != nil {
		return nil, appErrors.NewValidationError("invalid request format: " + err.Error())
	}

	req.PreviewID = strings.TrimSpace(req.PreviewID)
	if req.PreviewID == "" {
		return nil, appErrors.NewValidationError("preview_id is mandatory")
	}

	req.InterventionID = strings.TrimSpace(req.InterventionID)
	if req.InterventionID == "" {
		return nil, appErrors.NewValidationError("intervention_id is mandatory")
	}

	req.CSRFToken = strings.TrimSpace(req.CSRFToken)
	if req.CSRFToken == "" {
		return nil, appErrors.NewValidationError("csrf_token is mandatory")
	}

	req.OperatorID = strings.TrimSpace(req.OperatorID)
	if req.OperatorID == "" {
		return nil, appErrors.NewValidationError("operator_id is mandatory")
	}

	if req.SnapshotVersion <= 0 {
		return nil, appErrors.NewValidationError("snapshot_version must be strictly positive")
	}

	if req.SecondApproverID != nil {
		trimmed := strings.TrimSpace(*req.SecondApproverID)
		if trimmed == "" {
			req.SecondApproverID = nil
		} else {
			req.SecondApproverID = &trimmed
		}
	}

	return &req, nil
}

package contracts

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type ScopeType string

const (
	ScopeTenantAdmin ScopeType = "TENANT_ADMIN"
	ScopeSupport     ScopeType = "SUPPORT"
	ScopeOperator    ScopeType = "OPERATOR"
)

type SessionStatus string

const (
	SessionStatusConnecting    SessionStatus = "CONNECTING"
	SessionStatusActive        SessionStatus = "ACTIVE"
	SessionStatusStalePaused   SessionStatus = "STALE_PAUSED"
	SessionStatusRevoked       SessionStatus = "REVOKED"
	SessionStatusTerminated    SessionStatus = "TERMINATED"
)

type VisualState string

const (
	VisualStateSucceeded          VisualState = "SUCCEEDED"
	VisualStateRunning            VisualState = "RUNNING"
	VisualStateWaiting            VisualState = "WAITING"
	VisualStateRetryScheduled      VisualState = "RETRY_SCHEDULED"
	VisualStateUnknown            VisualState = "UNKNOWN"
	VisualStateRejected           VisualState = "REJECTED"
	VisualStateFailed             VisualState = "FAILED"
	VisualStateCompensating       VisualState = "COMPENSATING"
	VisualStateCompensated        VisualState = "COMPENSATED"
	VisualStateCompensationFailed VisualState = "COMPENSATION_FAILED"
	VisualStateSkipped            VisualState = "SKIPPED"
	VisualStateNotReached         VisualState = "NOT_REACHED"
	VisualStatePaused             VisualState = "PAUSED"
	VisualStateNeedsAttention     VisualState = "NEEDS_ATTENTION"
)

type ActionType string

const (
	ActionPause           ActionType = "PAUSE"
	ActionResume          ActionType = "RESUME"
	ActionRetryStep       ActionType = "RETRY_STEP"
	ActionForceCompensate ActionType = "FORCE_COMPENSATE"
	ActionMarkCompleted   ActionType = "MARK_COMPLETED"
	ActionSkipStep        ActionType = "SKIP_STEP"
)

type ActionStatus string

const (
	ActionStatusPendingConfirmation ActionStatus = "PENDING_CONFIRMATION"
	ActionStatusConfirmed           ActionStatus = "CONFIRMED"
	ActionStatusExpired             ActionStatus = "EXPIRED"
	ActionStatusMismatchRejected    ActionStatus = "MISMATCH_REJECTED"
)

type DashboardStreamSession struct {
	ID                     uuid.UUID     `json:"id"`
	PublicID               string        `json:"public_id"`
	TenantID               string        `json:"tenant_id"`
	Source                 string        `json:"source"`
	ExternalID             string        `json:"external_id"`
	CorrelationID          uuid.UUID     `json:"correlation_id"`
	PrincipalID            string        `json:"principal_id"`
	ScopeType              ScopeType     `json:"scope_type"`
	ImpersonationTicket    *string       `json:"impersonation_ticket,omitempty"`
	ImpersonationExpiresAt *time.Time    `json:"impersonation_expires_at,omitempty"`
	ReauthDeadline         time.Time     `json:"reauth_deadline"`
	LastHeartbeatAt        time.Time     `json:"last_heartbeat_at"`
	Status                 SessionStatus `json:"status"`
	CreatedAt              time.Time     `json:"created_at"`
	UpdatedAt              time.Time     `json:"updated_at"`
	Version                int64         `json:"version"`
}

type DashboardTileSnapshot struct {
	ID              uuid.UUID       `json:"id"`
	PublicID        string          `json:"public_id"`
	TenantID        string          `json:"tenant_id"`
	Source          string          `json:"source"`
	ExternalID      string          `json:"external_id"`
	CorrelationID   uuid.UUID       `json:"correlation_id"`
	TileKey         string          `json:"tile_key"`
	MetricsPayload  json.RawMessage `json:"metrics_payload"`
	DataAgeMs       int64           `json:"data_age_ms"`
	IsStale         bool            `json:"is_stale"`
	AsOfVersion     int64           `json:"as_of_version"`
	SourceTimestamp time.Time       `json:"source_timestamp"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	Version         int64           `json:"version"`
}

type DashboardActionPreview struct {
	ID                     uuid.UUID       `json:"id"`
	PublicID               string          `json:"public_id"`
	TenantID               string          `json:"tenant_id"`
	Source                 string          `json:"source"`
	ExternalID             string          `json:"external_id"`
	CorrelationID          uuid.UUID       `json:"correlation_id"`
	SagaID                 string          `json:"saga_id"`
	ActionType             ActionType      `json:"action_type"`
	SnapshotVersion        int64           `json:"snapshot_version"`
	RequiresSecondApprover bool            `json:"requires_second_approver"`
	MoneyAtStake           float64         `json:"money_at_stake"`
	PreviewPayload         json.RawMessage `json:"preview_payload"`
	Status                 ActionStatus    `json:"status"`
	ExpiresAt              time.Time       `json:"expires_at"`
	CreatedAt              time.Time       `json:"created_at"`
	UpdatedAt              time.Time       `json:"updated_at"`
	Version                int64           `json:"version"`
}

type DashboardAuditEvent struct {
	ID            uuid.UUID       `json:"id"`
	PublicID      string          `json:"public_id"`
	TenantID      string          `json:"tenant_id"`
	Source        string          `json:"source"`
	ExternalID    string          `json:"external_id"`
	CorrelationID uuid.UUID       `json:"correlation_id"`
	OperatorID    string          `json:"operator_id"`
	EventType     string          `json:"event_type"`
	ResourceID    string          `json:"resource_id"`
	Details       json.RawMessage `json:"details"`
	IPAddress     string          `json:"ip_address"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	Version       int64           `json:"version"`
}

type StreamEvent struct {
	ID           string          `json:"id"`
	Event        string          `json:"event"`
	Data         json.RawMessage `json:"data"`
	RetryHintMs  int64           `json:"retry_hint_ms,omitempty"`
	TimestampUTC time.Time       `json:"timestamp_utc"`
}

type ConnectStreamRequest struct {
	PrincipalID         string    `json:"principal_id"`
	ScopeType           ScopeType `json:"scope_type"`
	ImpersonationTicket *string   `json:"impersonation_ticket,omitempty"`
	RequestedTenant     string    `json:"requested_tenant,omitempty"`
}

type OverviewResponse struct {
	TenantScope   string                  `json:"tenant_scope"`
	Tiles         []DashboardTileSnapshot `json:"tiles"`
	OverallBanner string                  `json:"overall_banner,omitempty"`
	DataAgeMs     int64                   `json:"data_age_ms"`
	Timestamp     time.Time               `json:"timestamp"`
}

type PreviewActionRequest struct {
	SagaID             string     `json:"saga_id"`
	ActionType         ActionType `json:"action_type"`
	CurrentSagaVersion int64      `json:"current_saga_version"`
	OperatorID         string     `json:"operator_id"`
}

type PreviewActionResponse struct {
	PreviewID              string          `json:"preview_id"`
	SagaID                 string          `json:"saga_id"`
	ActionType             ActionType      `json:"action_type"`
	SnapshotVersion        int64           `json:"snapshot_version"`
	RequiresSecondApprover bool            `json:"requires_second_approver"`
	MoneyAtStake           float64         `json:"money_at_stake"`
	AffectedSteps          []string        `json:"affected_steps"`
	Commands               []string        `json:"commands"`
	Compensations          []string        `json:"compensations"`
	CustomerImpact         string          `json:"customer_impact"`
	ExpiresAt              time.Time       `json:"expires_at"`
	Details                json.RawMessage `json:"details"`
}

type ExecuteActionRequest struct {
	PreviewID        string  `json:"preview_id"`
	InterventionID   string  `json:"intervention_id"`
	SnapshotVersion  int64   `json:"snapshot_version"`
	CSRFToken        string  `json:"csrf_token"`
	OperatorID       string  `json:"operator_id"`
	SecondApproverID *string `json:"second_approver_id,omitempty"`
}

type ExecuteActionResponse struct {
	Success        bool       `json:"success"`
	InterventionID string     `json:"intervention_id"`
	ActionType     ActionType `json:"action_type"`
	SagaID         string     `json:"saga_id"`
	ExecutedAt     time.Time  `json:"executed_at"`
}


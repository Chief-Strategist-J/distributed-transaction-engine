package contracts

import (
	"time"

	"github.com/google/uuid"
)

type IdempotencyState string

const (
	StateInProgress    IdempotencyState = "IN_PROGRESS"
	StateCompleted     IdempotencyState = "COMPLETED"
	StateRejectedFinal IdempotencyState = "REJECTED_FINAL"
	StateUnknown       IdempotencyState = "UNKNOWN"
)

type IdempotencyScope struct {
	TenantID       string `json:"tenantId"`
	PrincipalID    string `json:"principalId"`
	OperationID    string `json:"operationId"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type ResponseSnapshot struct {
	StatusCode int                    `json:"statusCode"`
	Headers    map[string]string      `json:"headers"`
	Body       map[string]interface{} `json:"body"`
}

type IdempotencyRecord struct {
	ID                   uuid.UUID         `json:"id"`
	PublicID             string            `json:"publicId"`
	TenantID             string            `json:"tenantId"`
	Source               string            `json:"source"`
	ExternalID           string            `json:"externalId"`
	CorrelationID        uuid.UUID         `json:"correlationId"`
	PrincipalID          string            `json:"principalId"`
	OperationID          string            `json:"operationId"`
	IdempotencyKey       string            `json:"idempotencyKey"`
	RequestFingerprint   string            `json:"requestFingerprint"`
	CurrentState         IdempotencyState  `json:"currentState"`
	ResponseStatus       *int              `json:"responseStatus,omitempty"`
	ResponseHeaders      map[string]string `json:"responseHeaders,omitempty"`
	ResponseBody         map[string]any    `json:"responseBody,omitempty"`
	ResourceReference    string            `json:"resourceReference,omitempty"`
	LeaseOwner           string            `json:"leaseOwner,omitempty"`
	LeaseUntil           *time.Time        `json:"leaseUntil,omitempty"`
	RecoveryFencingToken int64             `json:"recoveryFencingToken"`
	CreatedAt            time.Time         `json:"createdAt"`
	UpdatedAt            time.Time         `json:"updatedAt"`
	CompletedAt          *time.Time        `json:"completedAt,omitempty"`
	ExpiresAt            time.Time         `json:"expiresAt"`
}

type AcquireOutcome string

const (
	OutcomeAcquired             AcquireOutcome = "ACQUIRED"
	OutcomeReplay               AcquireOutcome = "REPLAY"
	OutcomeConflictInProgress   AcquireOutcome = "CONFLICT_IN_PROGRESS"
	OutcomeFingerprintMismatch  AcquireOutcome = "FINGERPRINT_MISMATCH"
)

type AcquireResult struct {
	Outcome  AcquireOutcome     `json:"outcome"`
	Record   *IdempotencyRecord `json:"record,omitempty"`
	Snapshot *ResponseSnapshot  `json:"snapshot,omitempty"`
}

type IngestIdempotencyInput struct {
	ID             string                 `json:"id"`
	PublicID       string                 `json:"publicId"`
	TenantID       string                 `json:"tenantId"`
	Source         string                 `json:"source"`
	ExternalID     string                 `json:"externalId"`
	CorrelationID  string                 `json:"correlationId"`
	PrincipalID    string                 `json:"principalId"`
	OperationID    string                 `json:"operationId"`
	IdempotencyKey string                 `json:"idempotencyKey"`
	Payload        map[string]interface{} `json:"payload"`
}


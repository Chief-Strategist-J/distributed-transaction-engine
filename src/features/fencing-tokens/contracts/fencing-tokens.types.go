package contracts

import (
	"time"

	"github.com/google/uuid"
)

type FencingLease struct {
	ID              uuid.UUID  `json:"id"`
	PublicID        string     `json:"publicId"`
	TenantID        string     `json:"tenantId"`
	Source          string     `json:"source"`
	ExternalID      string     `json:"externalId"`
	CorrelationID   uuid.UUID  `json:"correlationId"`
	ResourceID      string     `json:"resourceId"`
	HolderID        string     `json:"holderId"`
	Epoch           int64      `json:"epoch"`
	LeaseDurationMs int        `json:"leaseDurationMs"`
	AcquiredAt      time.Time  `json:"acquiredAt"`
	ExpiresAt       time.Time  `json:"expiresAt"`
	ReleasedAt      *time.Time `json:"releasedAt,omitempty"`
	CreatedAt       time.Time  `json:"createdAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
}

type FencingAudit struct {
	ID             uuid.UUID `json:"id"`
	PublicID       string    `json:"publicId"`
	TenantID       string    `json:"tenantId"`
	ResourceID     string    `json:"resourceId"`
	AttemptedToken int64     `json:"attemptedToken"`
	CurrentToken   int64     `json:"currentToken"`
	RejectedHolder string    `json:"rejectedHolder"`
	ViolationType  string    `json:"violationType"`
	RecordedAt     time.Time `json:"recordedAt"`
}

type AcquireLeaseInput struct {
	TenantID        string `json:"tenantId"`
	ResourceID      string `json:"resourceId"`
	HolderID        string `json:"holderId"`
	LeaseDurationMs int    `json:"leaseDurationMs"`
}

type RenewLeaseInput struct {
	TenantID        string `json:"tenantId"`
	ResourceID      string `json:"resourceId"`
	HolderID        string `json:"holderId"`
	Epoch           int64  `json:"epoch"`
	LeaseDurationMs int    `json:"leaseDurationMs"`
}

type ReleaseLeaseInput struct {
	TenantID   string `json:"tenantId"`
	ResourceID string `json:"resourceId"`
	HolderID   string `json:"holderId"`
	Epoch      int64  `json:"epoch"`
}

type FencedBarrierInput struct {
	TenantID   string `json:"tenantId"`
	ResourceID string `json:"resourceId"`
	Token      int64  `json:"token"`
}

type FencingDecision struct {
	Allowed        bool   `json:"allowed"`
	StoredToken    int64  `json:"storedToken"`
	AttemptedToken int64  `json:"attemptedToken"`
	Reason         string `json:"reason"`
	ViolationType  string `json:"violationType,omitempty"`
}


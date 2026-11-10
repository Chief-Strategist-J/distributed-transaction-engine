package contracts

import (
	"time"

	"github.com/google/uuid"
)

type AnomalyType string

const (
	AnomalyPhantomSettlement AnomalyType = "PHANTOM_SETTLEMENT"
	AnomalyGhostTransaction  AnomalyType = "GHOST_TRANSACTION"
	AnomalyNumericalDrift    AnomalyType = "NUMERICAL_DRIFT"
	AnomalyStatusMismatch    AnomalyType = "STATUS_MISMATCH"
)

type ReconciliationStatus string

const (
	StatusAnomalyDetected     ReconciliationStatus = "ANOMALY_DETECTED"
	StatusRepairScheduled     ReconciliationStatus = "REPAIR_SCHEDULED"
	StatusAutoResolved        ReconciliationStatus = "AUTO_RESOLVED"
	StatusQuarantinedForAudit ReconciliationStatus = "QUARANTINED_FOR_AUDIT"
	StatusManualDismissed     ReconciliationStatus = "MANUAL_DISMISSED"
)

type ReconciliationStateTransitionEvent string

const (
	EventScheduleRepair  ReconciliationStateTransitionEvent = "SCHEDULE_REPAIR"
	EventAutoResolve     ReconciliationStateTransitionEvent = "AUTO_RESOLVE"
	EventEscalateAudit   ReconciliationStateTransitionEvent = "ESCALATE_AUDIT"
	EventManualDismiss   ReconciliationStateTransitionEvent = "MANUAL_DISMISS"
)

type ReconciliationAnomalyRecord struct {
	ID                  uuid.UUID            `json:"id"`
	PublicID            string               `json:"publicId"`
	TenantID            string               `json:"tenantId"`
	Source              string               `json:"source"`
	ExternalID          string               `json:"externalId"`
	CorrelationID       uuid.UUID            `json:"correlationId"`
	SagaID              uuid.UUID            `json:"sagaId"`
	AnomalyType         AnomalyType          `json:"anomalyType"`
	Status              ReconciliationStatus `json:"status"`
	InternalStatus      string               `json:"internalStatus"`
	ExternalStatus      string               `json:"externalStatus"`
	InternalAmountCents int64                `json:"internalAmountCents"`
	ExternalAmountCents int64                `json:"externalAmountCents"`
	Currency            string               `json:"currency"`
	DiscrepancyReason   string               `json:"discrepancyReason"`
	ResolutionAction    string               `json:"resolutionAction"`
	Version             int64                `json:"version"`
	CreatedAt           time.Time            `json:"createdAt"`
	UpdatedAt           time.Time            `json:"updatedAt"`
}

type EvaluateReconciliationInput struct {
	TenantID            string    `json:"tenantId"`
	SagaID              uuid.UUID `json:"sagaId"`
	InternalStatus      string    `json:"internalStatus"`
	ExternalStatus      string    `json:"externalStatus"`
	InternalAmountCents int64     `json:"internalAmountCents"`
	ExternalAmountCents int64     `json:"externalAmountCents"`
	Currency            string    `json:"currency"`
}

type ReconciliationEvaluationResult struct {
	IsAnomaly           bool         `json:"isAnomaly"`
	AnomalyType         *AnomalyType `json:"anomalyType,omitempty"`
	CanAutoRepair       bool         `json:"canAutoRepair"`
	SuggestedAction     string       `json:"suggestedAction"`
	DiscrepancyReason   string       `json:"discrepancyReason"`
}

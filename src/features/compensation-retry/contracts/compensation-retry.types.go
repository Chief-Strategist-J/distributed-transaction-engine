package contracts

import (
	"time"

	"github.com/google/uuid"
)

type OutcomeClass string

const (
	OutcomeDone                OutcomeClass = "DONE"
	OutcomeRetry               OutcomeClass = "RETRY"
	OutcomeRateLimited         OutcomeClass = "RATE_LIMITED"
	OutcomeWaitAndPoll         OutcomeClass = "WAIT_AND_POLL"
	OutcomeAmbiguous           OutcomeClass = "AMBIGUOUS"
	OutcomeContractDefect      OutcomeClass = "CONTRACT_DEFECT"
	OutcomeCredential          OutcomeClass = "CREDENTIAL"
	OutcomeBusinessAlternative OutcomeClass = "BUSINESS_ALTERNATIVE"
	OutcomeDependencyDown      OutcomeClass = "DEPENDENCY_DOWN"
	OutcomeNeedsAttention      OutcomeClass = "NEEDS_ATTENTION"
)

type EscalationLevel string

const (
	EscalationNone          EscalationLevel = "NONE"
	EscalationLevel1Early   EscalationLevel = "LEVEL_1_EARLY_WARNING"
	EscalationLevel2Deadline EscalationLevel = "LEVEL_2_DEADLINE_RISK"
	EscalationLevel3Missed  EscalationLevel = "LEVEL_3_DEADLINE_MISSED"
)

type CompensationRetryRecord struct {
	ID               uuid.UUID  `json:"id"`
	PublicID         string     `json:"publicId"`
	TenantID         string     `json:"tenantId"`
	Source           string     `json:"source"`
	ExternalID       string     `json:"externalId"`
	CorrelationID    uuid.UUID  `json:"correlationId"`
	SagaID           uuid.UUID  `json:"sagaId"`
	StepID           string     `json:"stepId"`
	StepType         string     `json:"stepType"`
	DependencyID     string     `json:"dependencyId"`
	AttemptNumber    int        `json:"attemptNumber"`
	MaxAttempts      int        `json:"maxAttempts"`
	FirstAttemptAt   time.Time  `json:"firstAttemptAt"`
	BusinessDeadline time.Time  `json:"businessDeadline"`
	NextRetryAt      *time.Time `json:"nextRetryAt,omitempty"`
	Status           string     `json:"status"`
	LastError        string     `json:"lastError"`
	Version          int64      `json:"version"`
	CreatedAt        time.Time  `json:"createdAt"`
	UpdatedAt        time.Time  `json:"updatedAt"`
}

type EvaluateCompensationRetryInput struct {
	TenantID         string `json:"tenantId"`
	SagaID           string `json:"sagaId"`
	StepID           string `json:"stepId"`
	StepType         string `json:"stepType"`
	DependencyID     string `json:"dependencyId"`
	CompensationKey  string `json:"compensationKey"`
	AttemptNumber    int    `json:"attemptNumber"`
	StatusCode       int    `json:"statusCode"`
	ErrorMessage     string `json:"errorMessage"`
	RetryAfterHeader string `json:"retryAfterHeader,omitempty"`
	FirstAttemptAt   string `json:"firstAttemptAt"`
	BusinessDeadline string `json:"businessDeadline"`
	IsBreakerOpen    bool   `json:"isBreakerOpen,omitempty"`
}

type CompensationRetryDecision struct {
	Action          string          `json:"action"`
	OutcomeClass    OutcomeClass    `json:"outcomeClass"`
	DelayMillis     int64           `json:"delayMillis"`
	EscalationLevel EscalationLevel `json:"escalationLevel"`
	Reason          string          `json:"reason"`
}

type VerificationResult struct {
	Status    string `json:"status"`
	Reference string `json:"reference,omitempty"`
	Verified  bool   `json:"verified"`
}

package contracts

import (
	"time"

	"github.com/google/uuid"
)

type ErrorClass string

const (
	ClassNotSent               ErrorClass = "NOT_SENT"
	ClassTransientNotExecuted  ErrorClass = "TRANSIENT_NOT_EXECUTED"
	ClassAmbiguous             ErrorClass = "AMBIGUOUS"
	ClassRejected              ErrorClass = "REJECTED"
	ClassCredential            ErrorClass = "CREDENTIAL"
	ClassCausalWait            ErrorClass = "CAUSAL_WAIT"
	ClassDeterministicError    ErrorClass = "DETERMINISTIC_ERROR"
)

type BackoffAlgorithm string

const (
	AlgorithmFullJitter  BackoffAlgorithm = "EXPONENTIAL_FULL_JITTER"
	AlgorithmEqualJitter BackoffAlgorithm = "EXPONENTIAL_EQUAL_JITTER"
	AlgorithmLinear      BackoffAlgorithm = "LINEAR_BACKOFF"
)

type SagaPosition string

const (
	PositionCompensable SagaPosition = "COMPENSABLE"
	PositionPivot       SagaPosition = "PIVOT"
	PositionRetriable   SagaPosition = "RETRIABLE"
)

type StepCapability struct {
	IdempotentByKey bool   `json:"idempotentByKey"`
	StatusQuery     bool   `json:"statusQuery"`
	EffectType      string `json:"effectType"`
}

type RetryPolicyConfig struct {
	ID                 uuid.UUID        `json:"id"`
	PublicID           string           `json:"publicId"`
	TenantID           string           `json:"tenantId"`
	PolicyName         string           `json:"policyName"`
	MaxAttempts        int              `json:"maxAttempts"`
	MaxElapsedSeconds  int              `json:"maxElapsedSeconds"`
	BaseIntervalMillis int              `json:"baseIntervalMillis"`
	MaxIntervalMillis  int              `json:"maxIntervalMillis"`
	Algorithm          BackoffAlgorithm `json:"algorithm"`
}

type RetryDecisionAction string

const (
	ActionRetry            RetryDecisionAction = "RETRY"
	ActionHaltCompensate   RetryDecisionAction = "HALT_COMPENSATE"
	ActionNeedsAttention   RetryDecisionAction = "NEEDS_ATTENTION"
	ActionPostponeBreaker  RetryDecisionAction = "POSTPONE_BREAKER"
)

type RetryDecision struct {
	Action      RetryDecisionAction `json:"action"`
	Delay       time.Duration       `json:"delay"`
	DelayMillis int64               `json:"delayMillis"`
	ErrorClass  ErrorClass          `json:"errorClass"`
	Reason      string              `json:"reason"`
}

type EvaluateRetryInput struct {
	TenantID           string           `json:"tenantId"`
	SagaID             string           `json:"sagaId"`
	StepID             string           `json:"stepId"`
	AttemptNumber      int              `json:"attemptNumber"`
	StatusCode         int              `json:"statusCode"`
	ErrorMessage       string           `json:"errorMessage"`
	RetryAfterHeader   string           `json:"retryAfterHeader,omitempty"`
	Capability         StepCapability   `json:"capability"`
	Position           SagaPosition     `json:"position"`
	DependencyID       string           `json:"dependencyId"`
	FirstAttemptTime   string           `json:"firstAttemptTime"`
	EffectiveDeadline  string           `json:"effectiveDeadline"`
	FingerprintHistory []string         `json:"fingerprintHistory,omitempty"`
	Policy             RetryPolicyConfig `json:"policy"`
}


package contracts

type TransactionState string

const (
	StateCreated       TransactionState = "CREATED"
	StateRunning       TransactionState = "RUNNING"
	StateWaiting       TransactionState = "WAITING"
	StateThrottledWait TransactionState = "THROTTLED_WAIT"
	StateReconciling   TransactionState = "RECONCILING"
	StateCompensating  TransactionState = "COMPENSATING"
	StateStuck         TransactionState = "STUCK"
	StateCompleted     TransactionState = "COMPLETED"
	StateCancelled     TransactionState = "CANCELLED"
	StateCompensated   TransactionState = "COMPENSATED"
	StateFailed        TransactionState = "FAILED"
)

type StateTransitionEvent string

const (
	EventOrchestratorClaim   StateTransitionEvent = "ORCHESTRATOR_CLAIM"
	EventClientAbort         StateTransitionEvent = "CLIENT_ABORT"
	EventStepSuccess         StateTransitionEvent = "STEP_SUCCESS"
	EventDAGCompleted        StateTransitionEvent = "DAG_COMPLETED"
	EventBusinessFailure     StateTransitionEvent = "BUSINESS_FAILURE"
	EventRateLimited         StateTransitionEvent = "RATE_LIMITED"
	EventIndeterminateError  StateTransitionEvent = "INDETERMINATE_ERROR"
	EventThrottleTimerFired  StateTransitionEvent = "THROTTLE_TIMER_FIRED"
	EventProbeSucceeded      StateTransitionEvent = "PROBE_SUCCEEDED"
	EventProbeNotFound       StateTransitionEvent = "PROBE_NOT_FOUND"
	EventProbeBudgetExpired  StateTransitionEvent = "PROBE_BUDGET_EXPIRED"
	EventAsyncSuspend        StateTransitionEvent = "ASYNC_SUSPEND"
	EventWebhookApproved     StateTransitionEvent = "WEBHOOK_APPROVED"
	EventWebhookRejected     StateTransitionEvent = "WEBHOOK_REJECTED"
	EventWebhookTimeout      StateTransitionEvent = "WEBHOOK_TIMEOUT"
	EventCompensationStepOK  StateTransitionEvent = "COMPENSATION_STEP_OK"
	EventCompensationHalt    StateTransitionEvent = "COMPENSATION_HALT"
	EventCompensationDone    StateTransitionEvent = "COMPENSATION_DONE"
	EventOperatorRedrive     StateTransitionEvent = "OPERATOR_REDRIVE"
	EventOperatorWriteOff    StateTransitionEvent = "OPERATOR_WRITEOFF"
)

type SagaStateRecord struct {
	ID            string           `json:"id"`
	PublicID      string           `json:"publicId"`
	TenantID      string           `json:"tenantId"`
	Source        string           `json:"source"`
	ExternalID    string           `json:"externalId"`
	CorrelationID string           `json:"correlationId"`
	CurrentState  TransactionState `json:"currentState"`
	Version       int64            `json:"version"`
	FencingToken  int64            `json:"fencingToken"`
	CreatedAt     string           `json:"createdAt"`
	UpdatedAt     string           `json:"updatedAt"`
}

type StateTransitionAuditRecord struct {
	ID            string           `json:"id"`
	PublicID      string           `json:"publicId"`
	TenantID      string           `json:"tenantId"`
	SagaID        string           `json:"sagaId"`
	CorrelationID string           `json:"correlationId"`
	FromState     TransactionState `json:"fromState"`
	ToState       TransactionState `json:"toState"`
	VersionBefore int64            `json:"versionBefore"`
	VersionAfter  int64            `json:"versionAfter"`
	FencingToken  int64            `json:"fencingToken"`
	Reason        string           `json:"reason"`
	CreatedAt     string           `json:"createdAt"`
	UpdatedAt     string           `json:"updatedAt"`
}

type TransitionStateInput struct {
	TenantID        string               `json:"tenantId"`
	SagaID          string               `json:"sagaId"`
	Event           StateTransitionEvent `json:"event"`
	ExpectedVersion int64                `json:"expectedVersion"`
	FencingToken    int64                `json:"fencingToken"`
	Reason          string               `json:"reason"`
}

type InitializeStateInput struct {
	ID            string `json:"id"`
	PublicID      string `json:"publicId"`
	TenantID      string `json:"tenantId"`
	Source        string `json:"source"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
}

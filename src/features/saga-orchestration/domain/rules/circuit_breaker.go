package rules

import (
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"sync"
	"time"
)

type CircuitState string

const (
	CircuitStateClosed   CircuitState = "CLOSED"
	CircuitStateOpen     CircuitState = "OPEN"
	CircuitStateHalfOpen CircuitState = "HALF_OPEN"
)

type ActionCircuitBreaker struct {
	mu           sync.Mutex
	failureCount int
	threshold    int
	cooldown     time.Duration
	state        CircuitState
	lastFailedAt time.Time
	concurrency  int
	maxBulkhead  int
}

func NewActionCircuitBreaker(threshold int, cooldown time.Duration, maxBulkhead int) *ActionCircuitBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}
	if maxBulkhead <= 0 {
		maxBulkhead = 20
	}
	return &ActionCircuitBreaker{
		threshold:   threshold,
		cooldown:    cooldown,
		maxBulkhead: maxBulkhead,
		state:       CircuitStateClosed,
	}
}

type CircuitBreakerRegistry struct {
	mu       sync.RWMutex
	breakers map[string]*ActionCircuitBreaker
}

func NewCircuitBreakerRegistry() *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{
		breakers: make(map[string]*ActionCircuitBreaker),
	}
}

func (r *CircuitBreakerRegistry) GetBreaker(actionKey string) *ActionCircuitBreaker {
	defer observability.TraceEnter(nil)()
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.breakers[actionKey]; ok {
		return b
	}
	b := NewActionCircuitBreaker(5, 10*time.Second, 20)
	r.breakers[actionKey] = b
	return b
}

func (cb *ActionCircuitBreaker) AcquirePermission() (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now().UTC()
	if cb.state == CircuitStateOpen {
		if now.Sub(cb.lastFailedAt) > cb.cooldown {
			cb.state = CircuitStateHalfOpen
		} else {
			return errors.NewInternalError("circuit breaker open for action")
		}
	}

	if cb.concurrency >= cb.maxBulkhead {
		return errors.NewInternalError("bulkhead capacity exceeded for action")
	}

	cb.concurrency++
	return nil
}

func (cb *ActionCircuitBreaker) ReleasePermission(err error) {
	defer observability.TraceEnter(nil)()
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.concurrency > 0 {
		cb.concurrency--
	}

	if err != nil {
		cb.failureCount++
		cb.lastFailedAt = time.Now().UTC()
		if cb.failureCount >= cb.threshold || cb.state == CircuitStateHalfOpen {
			cb.state = CircuitStateOpen
		}
	} else {
		cb.failureCount = 0
		cb.state = CircuitStateClosed
	}
}

func (cb *ActionCircuitBreaker) GetState() CircuitState {
	defer observability.TraceEnter(nil)()
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

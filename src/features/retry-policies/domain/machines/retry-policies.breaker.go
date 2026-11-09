package machines

import (
	"sync"
	"time"
)

type BreakerState string

const (
	BreakerClosed   BreakerState = "CLOSED"
	BreakerOpen     BreakerState = "OPEN"
	BreakerHalfOpen BreakerState = "HALF_OPEN"
)

type DependencyBreaker struct {
	State            BreakerState
	FailureCount     int
	SuccessCount     int
	FailureThreshold int
	SuccessThreshold int
	Cooldown         time.Duration
	LastStateChange  time.Time
}

type CircuitBreakerManager struct {
	mu       sync.Mutex
	breakers map[string]*DependencyBreaker
}

func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*DependencyBreaker),
	}
}

func (m *CircuitBreakerManager) getOrCreate(depID string) *DependencyBreaker {
	b, exists := m.breakers[depID]
	if !exists {
		b = &DependencyBreaker{
			State:            BreakerClosed,
			FailureThreshold: 5,
			SuccessThreshold: 2,
			Cooldown:         10 * time.Second,
			LastStateChange:  time.Now().UTC(),
		}
		m.breakers[depID] = b
	}
	return b
}

func (m *CircuitBreakerManager) AllowRequest(depID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.getOrCreate(depID)
	now := time.Now().UTC()

	switch b.State {
	case BreakerClosed:
		return true

	case BreakerOpen:
		if now.Sub(b.LastStateChange) >= b.Cooldown {
			b.State = BreakerHalfOpen
			b.SuccessCount = 0
			b.LastStateChange = now
			return true
		}
		return false

	case BreakerHalfOpen:
		return true

	default:
		return true
	}
}

func (m *CircuitBreakerManager) RecordSuccess(depID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.getOrCreate(depID)
	if b.State == BreakerHalfOpen {
		b.SuccessCount++
		if b.SuccessCount >= b.SuccessThreshold {
			b.State = BreakerClosed
			b.FailureCount = 0
			b.SuccessCount = 0
			b.LastStateChange = time.Now().UTC()
		}
	} else if b.State == BreakerClosed {
		b.FailureCount = 0
	}
}

func (m *CircuitBreakerManager) RecordFailure(depID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.getOrCreate(depID)
	now := time.Now().UTC()

	if b.State == BreakerHalfOpen {
		b.State = BreakerOpen
		b.LastStateChange = now
		return
	}

	if b.State == BreakerClosed {
		b.FailureCount++
		if b.FailureCount >= b.FailureThreshold {
			b.State = BreakerOpen
			b.LastStateChange = now
		}
	}
}

func (m *CircuitBreakerManager) GetState(depID string) BreakerState {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.getOrCreate(depID)
	now := time.Now().UTC()
	if b.State == BreakerOpen && now.Sub(b.LastStateChange) >= b.Cooldown {
		return BreakerHalfOpen
	}
	return b.State
}

func (m *CircuitBreakerManager) Reset(depID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.getOrCreate(depID)
	b.State = BreakerClosed
	b.FailureCount = 0
	b.SuccessCount = 0
	b.LastStateChange = time.Now().UTC()
}


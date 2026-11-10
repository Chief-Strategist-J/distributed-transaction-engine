package adapters

import (
	"context"
	"distributed-transaction-engine/src/infra/observability"
	"sync"
)

type MemorySequentialDispatcherAdapter struct {
	mu          sync.Mutex
	Dispatched  []int
	Compensated []int
	Scheduled   []int
}

func NewMemorySequentialDispatcherAdapter() *MemorySequentialDispatcherAdapter {
	return &MemorySequentialDispatcherAdapter{}
}

func (a *MemorySequentialDispatcherAdapter) DispatchStep(_ context.Context, _ string, _ string, stepIndex int, _ map[string]interface{}) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Dispatched = append(a.Dispatched, stepIndex)
	return nil
}

func (a *MemorySequentialDispatcherAdapter) CompensateStep(_ context.Context, _ string, _ string, stepIndex int) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Compensated = append(a.Compensated, stepIndex)
	return nil
}

func (a *MemorySequentialDispatcherAdapter) ScheduleTimer(_ context.Context, _ string, _ string, stepIndex int, _ int) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Scheduled = append(a.Scheduled, stepIndex)
	return nil
}

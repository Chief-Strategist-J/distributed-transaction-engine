package chassis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Component struct {
	Name  string
	Start func(ctx context.Context) error
	Drain func(ctx context.Context) error
}

type Lifecycle struct {
	mu         sync.Mutex
	components []Component
	stopChan   chan struct{}
	stopped    bool
}

func NewLifecycle() *Lifecycle {
	return &Lifecycle{
		components: make([]Component, 0),
		stopChan:   make(chan struct{}),
	}
}

func (l *Lifecycle) AddComponent(name string, start func(ctx context.Context) error, drain func(ctx context.Context) error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.components = append(l.components, Component{
		Name:  name,
		Start: start,
		Drain: drain,
	})
}

func (l *Lifecycle) StartAll(ctx context.Context, fatalChan chan<- error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, c := range l.components {
		comp := c
		go func() {
			if err := comp.Start(ctx); err != nil {
				fatalChan <- fmt.Errorf("component '%s' failed: %w", comp.Name, err)
			}
		}()
	}
}

func (l *Lifecycle) StopAll(timeout time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.stopped {
		return nil
	}
	l.stopped = true
	close(l.stopChan)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	errChan := make(chan error, len(l.components))

	for _, c := range l.components {
		comp := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			if comp.Drain != nil {
				if err := comp.Drain(ctx); err != nil {
					errChan <- fmt.Errorf("drain error on '%s': %w", comp.Name, err)
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	var drainErrs []error
	for err := range errChan {
		drainErrs = append(drainErrs, err)
	}

	return errors.Join(drainErrs...)
}

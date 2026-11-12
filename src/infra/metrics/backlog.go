package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"distributed-transaction-engine/src/shared/utils"
)

const (
	queryTimeout = 3 * time.Second

	outboxPendingQuery = `SELECT COUNT(*), COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)
FROM outbox_events
WHERE status = 'PENDING' AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())`

	sagaBacklogQuery = `SELECT COUNT(*)
FROM saga_instances
WHERE current_state IN ('CREATED', 'RUNNING', 'ROLLING_FORWARD', 'COMPENSATING')
  AND (lease_owner = '' OR lease_until IS NULL OR lease_until < NOW())`
)

type Backlog struct {
	OutboxPending         float64
	OutboxOldestAgeSecond float64
	SagaBacklog           float64
}

type Collector struct {
	db        *sql.DB
	freshness time.Duration
	mu        sync.Mutex
	cached    Backlog
	fetchedAt time.Time
}

func NewCollector(db *sql.DB, freshness time.Duration) *Collector {
	return &Collector{db: db, freshness: freshness}
}

func (c *Collector) Collect(ctx context.Context) (Backlog, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.fetchedAt.IsZero() && time.Since(c.fetchedAt) < c.freshness {
		return c.cached, nil
	}
	queryCtx, cancel := context.WithTimeout(utils.WithPlatform(ctx), queryTimeout)
	defer cancel()
	var backlog Backlog
	if err := c.db.QueryRowContext(queryCtx, outboxPendingQuery).Scan(&backlog.OutboxPending, &backlog.OutboxOldestAgeSecond); err != nil {
		return Backlog{}, fmt.Errorf("outbox backlog: %w", err)
	}
	if err := c.db.QueryRowContext(queryCtx, sagaBacklogQuery).Scan(&backlog.SagaBacklog); err != nil {
		return Backlog{}, fmt.Errorf("saga backlog: %w", err)
	}
	c.cached, c.fetchedAt = backlog, time.Now()
	return backlog, nil
}

func Render(writer io.Writer, backlog Backlog) {
	gauges := []struct {
		name  string
		help  string
		value float64
	}{
		{"dtx_outbox_pending_events", "Outbox events eligible for relay.", backlog.OutboxPending},
		{"dtx_outbox_oldest_pending_age_seconds", "Age of the oldest outbox event eligible for relay.", backlog.OutboxOldestAgeSecond},
		{"dtx_saga_backlog", "Active sagas without a live lease.", backlog.SagaBacklog},
	}
	for _, gauge := range gauges {
		fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", gauge.name, gauge.help, gauge.name, gauge.name, gauge.value)
	}
}

func (c *Collector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backlog, err := c.Collect(r.Context())
		if err != nil {
			http.Error(w, "backlog metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		Render(w, backlog)
	})
}

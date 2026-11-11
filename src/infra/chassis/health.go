package chassis

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"distributed-transaction-engine/src/shared/constants"
)

type HealthCoordinator struct {
	isLive  atomic.Bool
	isReady atomic.Bool
	db      *sql.DB
	brokers []string
}

const dependencyProbeTimeout = 2 * time.Second

func ParseBrokers(raw string) []string {
	brokers := make([]string, 0)
	for _, candidate := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	return brokers
}

func NewHealthCoordinator(db *sql.DB, brokers ...string) *HealthCoordinator {
	h := &HealthCoordinator{
		db:      db,
		brokers: brokers,
	}
	h.isLive.Store(true)
	h.isReady.Store(false)
	return h
}

func (h *HealthCoordinator) SetLive(val bool) {
	h.isLive.Store(val)
}

func (h *HealthCoordinator) SetReady(val bool) {
	h.isReady.Store(val)
}

func (h *HealthCoordinator) LiveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !h.isLive.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "down"})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": constants.StatusAlive})
	}
}

func (h *HealthCoordinator) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !h.isReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "not_ready"})
			return
		}
		if h.db != nil {
			pingCtx, pingCancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer pingCancel()
			if err := h.db.PingContext(pingCtx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]string{"status": constants.StatusDBUnreachable})
				return
			}
		}
		if len(h.brokers) > 0 && !anyBrokerReachable(r.Context(), h.brokers) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": constants.StatusKafkaUnreachable})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": constants.StatusReady})
	}
}

func anyBrokerReachable(ctx context.Context, brokers []string) bool {
	dialer := net.Dialer{Timeout: dependencyProbeTimeout}
	for _, broker := range brokers {
		probeCtx, cancel := context.WithTimeout(ctx, dependencyProbeTimeout)
		connection, err := dialer.DialContext(probeCtx, "tcp", broker)
		cancel()
		if err == nil {
			_ = connection.Close()
			return true
		}
	}
	return false
}

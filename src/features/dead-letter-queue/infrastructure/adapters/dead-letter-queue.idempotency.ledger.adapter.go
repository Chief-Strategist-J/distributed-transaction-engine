package adapters

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

const findExecutionStateQuery = `
SELECT current_state
FROM idempotency_keys
WHERE tenant_id = $1 AND idempotency_key = $2
ORDER BY created_at DESC
LIMIT 1`

type PostgresIdempotencyLedgerAdapter struct {
	db *sql.DB
}

func NewPostgresIdempotencyLedgerAdapter(db *sql.DB) *PostgresIdempotencyLedgerAdapter {
	return &PostgresIdempotencyLedgerAdapter{db: db}
}

func (a *PostgresIdempotencyLedgerAdapter) FindExecutionState(ctx context.Context, tenantID string, idempotencyKey string) (spanResult0 string, spanResult1 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var state string
	err := a.db.QueryRowContext(ctx, findExecutionStateQuery, tenantID, idempotencyKey).Scan(&state)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, appErrors.NewInternalError("failed to read idempotency ledger: " + err.Error())
	}
	return strings.ToUpper(strings.TrimSpace(state)), true, nil
}

type MemoryIdempotencyLedgerAdapter struct {
	mu     sync.RWMutex
	states map[string]string
}

func NewMemoryIdempotencyLedgerAdapter() *MemoryIdempotencyLedgerAdapter {
	return &MemoryIdempotencyLedgerAdapter{states: make(map[string]string)}
}

func (a *MemoryIdempotencyLedgerAdapter) Record(tenantID string, idempotencyKey string, state string) {
	defer observability.TraceEnter(nil)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.states[tenantID+"\x00"+idempotencyKey] = strings.ToUpper(state)
}

func (a *MemoryIdempotencyLedgerAdapter) FindExecutionState(_ context.Context, tenantID string, idempotencyKey string) (spanResult0 string, spanResult1 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	state, found := a.states[tenantID+"\x00"+idempotencyKey]
	return state, found, nil
}

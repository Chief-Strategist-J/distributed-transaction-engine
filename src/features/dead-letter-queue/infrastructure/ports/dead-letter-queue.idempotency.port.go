package ports

import "context"

type IdempotencyLedgerPort interface {
	FindExecutionState(ctx context.Context, tenantID string, idempotencyKey string) (state string, found bool, err error)
}

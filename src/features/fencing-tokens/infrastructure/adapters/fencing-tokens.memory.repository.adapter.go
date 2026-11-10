package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"sync"
	"time"
)

type MemoryFencingRepositoryAdapter struct {
	mu           sync.RWMutex
	leases       map[string]contracts.FencingLease
	storedTokens map[string]int64
	audits       []contracts.FencingAudit
}

var _ ports.RepositoryPort = (*MemoryFencingRepositoryAdapter)(nil)

func NewMemoryFencingRepositoryAdapter() *MemoryFencingRepositoryAdapter {
	return &MemoryFencingRepositoryAdapter{
		leases:       make(map[string]contracts.FencingLease),
		storedTokens: make(map[string]int64),
		audits:       make([]contracts.FencingAudit, 0),
	}
}

func (a *MemoryFencingRepositoryAdapter) AcquireOrTakeover(ctx context.Context, tenantID, resourceID, holderID string, durationMs int) (spanResult0 *contracts.FencingLease, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, resourceID)
	now := time.Now().UTC()
	existing, exists := a.leases[key]

	if exists && existing.ReleasedAt == nil && existing.ExpiresAt.After(now) {
		if existing.HolderID != holderID {
			return nil, appErrors.NewConflictError("lease currently held by active holder: " + existing.HolderID)
		}
	}

	newEpoch := int64(1)
	if exists {
		newEpoch = existing.Epoch + 1
	}

	rawUUID := uuid.New()
	pubID := fmt.Sprintf("fnl_%s", strings.ReplaceAll(rawUUID.String(), "-", "")[:26])
	expiresAt := now.Add(time.Duration(durationMs) * time.Millisecond)

	lease := contracts.FencingLease{
		ID:              rawUUID,
		PublicID:        pubID,
		TenantID:        tenantID,
		Source:          "ENGINE_LEASING",
		ExternalID:      resourceID,
		CorrelationID:   rawUUID,
		ResourceID:      resourceID,
		HolderID:        holderID,
		Epoch:           newEpoch,
		LeaseDurationMs: durationMs,
		AcquiredAt:      now,
		ExpiresAt:       expiresAt,
		ReleasedAt:      nil,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	a.leases[key] = lease
	return &lease, nil
}

func (a *MemoryFencingRepositoryAdapter) Renew(ctx context.Context, tenantID, resourceID, holderID string, epoch int64, durationMs int) (spanResult0 *contracts.FencingLease, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, resourceID)
	existing, exists := a.leases[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("lease not found")
	}

	if existing.ReleasedAt != nil {
		return nil, appErrors.NewConflictError("lease has already been released")
	}

	if existing.HolderID != holderID || existing.Epoch != epoch {
		return nil, appErrors.NewConflictError("lease ownership mismatch or stale epoch")
	}

	now := time.Now().UTC()
	existing.ExpiresAt = now.Add(time.Duration(durationMs) * time.Millisecond)
	existing.UpdatedAt = now

	a.leases[key] = existing
	return &existing, nil
}

func (a *MemoryFencingRepositoryAdapter) Release(ctx context.Context, tenantID, resourceID, holderID string, epoch int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, resourceID)
	existing, exists := a.leases[key]
	if !exists {
		return appErrors.NewNotFoundError("lease not found")
	}

	if existing.HolderID != holderID || existing.Epoch != epoch {
		return appErrors.NewConflictError("cannot release lease owned by another holder or epoch")
	}

	now := time.Now().UTC()
	existing.ReleasedAt = &now
	existing.UpdatedAt = now
	a.leases[key] = existing
	return nil
}

func (a *MemoryFencingRepositoryAdapter) GetLease(ctx context.Context, tenantID, resourceID string) (spanResult0 *contracts.FencingLease, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", tenantID, resourceID)
	existing, exists := a.leases[key]
	if !exists {
		return nil, appErrors.NewNotFoundError("lease not found")
	}
	return &existing, nil
}

func (a *MemoryFencingRepositoryAdapter) ExecuteFencedBarrier(ctx context.Context, tenantID, resourceID string, token int64) (spanResult0 *contracts.FencingDecision, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, resourceID)
	stored, exists := a.storedTokens[key]
	if !exists {
		stored = 0
	}

	if token >= stored {
		a.storedTokens[key] = token
		return &contracts.FencingDecision{
			Allowed:        true,
			StoredToken:    token,
			AttemptedToken: token,
			Reason:         "fencing barrier check passed",
		}, nil
	}

	return &contracts.FencingDecision{
		Allowed:        false,
		StoredToken:    stored,
		AttemptedToken: token,
		Reason:         "stale fencing token; zombie write suppressed",
		ViolationType:  "STALE_TOKEN",
	}, nil
}

func (a *MemoryFencingRepositoryAdapter) RecordAudit(ctx context.Context, audit contracts.FencingAudit) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()

	a.audits = append(a.audits, audit)
	return nil
}

func (a *MemoryFencingRepositoryAdapter) GetAudits(ctx context.Context, tenantID, resourceID string, limit int) (spanResult0 []contracts.FencingAudit, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make([]contracts.FencingAudit, 0)
	for i := len(a.audits) - 1; i >= 0; i-- {
		if a.audits[i].TenantID == tenantID && a.audits[i].ResourceID == resourceID {
			result = append(result, a.audits[i])
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

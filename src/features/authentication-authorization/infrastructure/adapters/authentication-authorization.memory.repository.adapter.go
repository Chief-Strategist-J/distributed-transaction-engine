package adapters

import (
	"context"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	"sync"
	"time"
)

type AuthMemoryRepositoryAdapter struct {
	mu                sync.RWMutex
	apiKeys           []contracts.APIKeyRecord
	serviceIdentities []contracts.ServiceIdentityRecord
	decisions         []contracts.AuthDecisionRecord
	revocations       []contracts.RevocationRecord
	replays           []contracts.ProviderReplayRecord
}

func NewAuthMemoryRepositoryAdapter() *AuthMemoryRepositoryAdapter {
	return &AuthMemoryRepositoryAdapter{
		apiKeys:           make([]contracts.APIKeyRecord, 0),
		serviceIdentities: make([]contracts.ServiceIdentityRecord, 0),
		decisions:         make([]contracts.AuthDecisionRecord, 0),
		revocations:       make([]contracts.RevocationRecord, 0),
		replays:           make([]contracts.ProviderReplayRecord, 0),
	}
}

func (a *AuthMemoryRepositoryAdapter) SaveAPIKey(ctx context.Context, record *contracts.APIKeyRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.apiKeys = append(a.apiKeys, *record)
	return nil
}

func (a *AuthMemoryRepositoryAdapter) GetAPIKeyByPrefix(ctx context.Context, tenantID string, keyPrefix string) (spanResult0 *contracts.APIKeyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := range a.apiKeys {
		if a.apiKeys[i].TenantID == tenantID && a.apiKeys[i].KeyPrefix == keyPrefix {
			rec := a.apiKeys[i]
			return &rec, nil
		}
	}
	return nil, nil
}

func (a *AuthMemoryRepositoryAdapter) UpdateAPIKeyStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.APIKeyStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.apiKeys {
		if a.apiKeys[i].TenantID == tenantID && a.apiKeys[i].ID == id {
			a.apiKeys[i].Status = status
			return nil
		}
	}
	return nil
}

func (a *AuthMemoryRepositoryAdapter) SaveServiceIdentity(ctx context.Context, record *contracts.ServiceIdentityRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.serviceIdentities = append(a.serviceIdentities, *record)
	return nil
}

func (a *AuthMemoryRepositoryAdapter) GetServiceIdentity(ctx context.Context, tenantID string, spiffeID string) (spanResult0 *contracts.ServiceIdentityRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := range a.serviceIdentities {
		if a.serviceIdentities[i].TenantID == tenantID && a.serviceIdentities[i].SPIFFEID == spiffeID {
			rec := a.serviceIdentities[i]
			return &rec, nil
		}
	}
	return nil, nil
}

func (a *AuthMemoryRepositoryAdapter) SaveAuthDecision(ctx context.Context, record *contracts.AuthDecisionRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.decisions = append(a.decisions, *record)
	return nil
}

func (a *AuthMemoryRepositoryAdapter) GetDecisionsByPrincipal(ctx context.Context, tenantID string, principalID string, limit int) (spanResult0 []contracts.AuthDecisionRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	var results []contracts.AuthDecisionRecord
	for i := len(a.decisions) - 1; i >= 0; i-- {
		if a.decisions[i].TenantID == tenantID && a.decisions[i].PrincipalID == principalID {
			results = append(results, a.decisions[i])
			if len(results) >= limit {
				break
			}
		}
	}
	return results, nil
}

func (a *AuthMemoryRepositoryAdapter) SaveRevocation(ctx context.Context, record *contracts.RevocationRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.revocations = append(a.revocations, *record)
	return nil
}

func (a *AuthMemoryRepositoryAdapter) IsRevoked(ctx context.Context, tenantID string, targetType contracts.RevocationTargetType, targetID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	now := time.Now().UTC()
	for _, r := range a.revocations {
		if r.TenantID == tenantID && r.TargetType == targetType && r.TargetID == targetID && r.ExpiresAt.After(now) {
			return true, nil
		}
	}
	return false, nil
}

func (a *AuthMemoryRepositoryAdapter) SaveReplayNonce(ctx context.Context, record *contracts.ProviderReplayRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.replays = append(a.replays, *record)
	return nil
}

func (a *AuthMemoryRepositoryAdapter) HasReplayNonce(ctx context.Context, tenantID string, provider string, nonce string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	a.mu.RLock()
	defer a.mu.RUnlock()
	now := time.Now().UTC()
	for _, r := range a.replays {
		if r.TenantID == tenantID && r.Provider == provider && r.Nonce == nonce && r.ExpiresAt.After(now) {
			return true, nil
		}
	}
	return false, nil
}

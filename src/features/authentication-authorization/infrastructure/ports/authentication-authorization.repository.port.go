package ports

import (
	"context"

	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"github.com/google/uuid"
)

type AuthRepositoryPort interface {
	SaveAPIKey(ctx context.Context, record *contracts.APIKeyRecord) error
	GetAPIKeyByPrefix(ctx context.Context, tenantID string, keyPrefix string) (*contracts.APIKeyRecord, error)
	UpdateAPIKeyStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.APIKeyStatus) error

	SaveServiceIdentity(ctx context.Context, record *contracts.ServiceIdentityRecord) error
	GetServiceIdentity(ctx context.Context, tenantID string, spiffeID string) (*contracts.ServiceIdentityRecord, error)

	SaveAuthDecision(ctx context.Context, record *contracts.AuthDecisionRecord) error
	GetDecisionsByPrincipal(ctx context.Context, tenantID string, principalID string, limit int) ([]contracts.AuthDecisionRecord, error)

	SaveRevocation(ctx context.Context, record *contracts.RevocationRecord) error
	IsRevoked(ctx context.Context, tenantID string, targetType contracts.RevocationTargetType, targetID string) (bool, error)

	SaveReplayNonce(ctx context.Context, record *contracts.ProviderReplayRecord) error
	HasReplayNonce(ctx context.Context, tenantID string, provider string, nonce string) (bool, error)
}


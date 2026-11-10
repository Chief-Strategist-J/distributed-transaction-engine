package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/features/authentication-authorization/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type AuthPostgresRepositoryAdapter struct {
	db *sql.DB
}

func NewAuthPostgresRepositoryAdapter(db *sql.DB) *AuthPostgresRepositoryAdapter {
	return &AuthPostgresRepositoryAdapter{db: db}
}

func (a *AuthPostgresRepositoryAdapter) SaveAPIKey(ctx context.Context, record *contracts.APIKeyRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_API_KEY,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.KeyPrefix, record.KeyHash, record.Name,
		pq.Array(record.Scopes), pq.Array(record.AllowedCIDRs), string(record.Status),
		record.ExpiresAt, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save api key: " + err.Error())
	}
	return nil
}

func (a *AuthPostgresRepositoryAdapter) GetAPIKeyByPrefix(ctx context.Context, tenantID string, keyPrefix string) (spanResult0 *contracts.APIKeyRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_API_KEY_BY_PREFIX, tenantID, keyPrefix)
	var rec contracts.APIKeyRecord
	var statusStr string
	var scopes []string
	var cidrs []string

	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.KeyPrefix, &rec.KeyHash, &rec.Name,
		pq.Array(&scopes), pq.Array(&cidrs), &statusStr, &rec.ExpiresAt, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get api key by prefix: " + err.Error())
	}
	rec.Scopes = scopes
	rec.AllowedCIDRs = cidrs
	rec.Status = contracts.APIKeyStatus(statusStr)
	return &rec, nil
}

func (a *AuthPostgresRepositoryAdapter) UpdateAPIKeyStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.APIKeyStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_API_KEY_STATUS, string(status), tenantID, id)
	if err != nil {
		return appErrors.NewInternalError("failed to update api key status: " + err.Error())
	}
	return nil
}

func (a *AuthPostgresRepositoryAdapter) SaveServiceIdentity(ctx context.Context, record *contracts.ServiceIdentityRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_SERVICE_IDENTITY,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.SPIFFEID, record.TrustDomain, pq.Array(record.AllowedMethods),
		record.Status, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save service identity: " + err.Error())
	}
	return nil
}

func (a *AuthPostgresRepositoryAdapter) GetServiceIdentity(ctx context.Context, tenantID string, spiffeID string) (spanResult0 *contracts.ServiceIdentityRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_SERVICE_IDENTITY, tenantID, spiffeID)
	var rec contracts.ServiceIdentityRecord
	var methods []string

	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.SPIFFEID, &rec.TrustDomain, pq.Array(&methods),
		&rec.Status, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get service identity: " + err.Error())
	}
	rec.AllowedMethods = methods
	return &rec, nil
}

func (a *AuthPostgresRepositoryAdapter) SaveAuthDecision(ctx context.Context, record *contracts.AuthDecisionRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_AUTH_DECISION,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, string(record.CallerType), record.PrincipalID, record.Action,
		record.ResourceID, string(record.Decision), record.ReasonCode, string(record.RiskLevel),
		record.StepUpVerified, record.ApproverID, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save auth decision: " + err.Error())
	}
	return nil
}

func (a *AuthPostgresRepositoryAdapter) GetDecisionsByPrincipal(ctx context.Context, tenantID string, principalID string, limit int) (spanResult0 []contracts.AuthDecisionRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_DECISIONS_BY_PRINCIPAL, tenantID, principalID, limit)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get auth decisions: " + err.Error())
	}
	defer rows.Close()

	var results []contracts.AuthDecisionRecord
	for rows.Next() {
		var rec contracts.AuthDecisionRecord
		var callerStr string
		var decisionStr string
		var riskStr string
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
			&rec.CorrelationID, &callerStr, &rec.PrincipalID, &rec.Action,
			&rec.ResourceID, &decisionStr, &rec.ReasonCode, &riskStr,
			&rec.StepUpVerified, &rec.ApproverID, &rec.CreatedAt,
		); err != nil {
			return nil, appErrors.NewInternalError("failed to scan decision row: " + err.Error())
		}
		rec.CallerType = contracts.CallerType(callerStr)
		rec.Decision = contracts.AuthDecision(decisionStr)
		rec.RiskLevel = contracts.RiskLevel(riskStr)
		results = append(results, rec)
	}
	return results, nil
}

func (a *AuthPostgresRepositoryAdapter) SaveRevocation(ctx context.Context, record *contracts.RevocationRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_REVOCATION,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, string(record.TargetType), record.TargetID, record.RevocationReason,
		record.RevokedAt, record.ExpiresAt, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save revocation: " + err.Error())
	}
	return nil
}

func (a *AuthPostgresRepositoryAdapter) IsRevoked(ctx context.Context, tenantID string, targetType contracts.RevocationTargetType, targetID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var exists bool
	err := a.db.QueryRowContext(ctx, queries.FLOW_CHECK_REVOCATION, tenantID, string(targetType), targetID).Scan(&exists)
	if err != nil {
		return false, appErrors.NewInternalError("failed to check revocation: " + err.Error())
	}
	return exists, nil
}

func (a *AuthPostgresRepositoryAdapter) SaveReplayNonce(ctx context.Context, record *contracts.ProviderReplayRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_REPLAY_NONCE,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.Provider, record.Nonce, record.ReceivedAt, record.ExpiresAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save replay nonce: " + err.Error())
	}
	return nil
}

func (a *AuthPostgresRepositoryAdapter) HasReplayNonce(ctx context.Context, tenantID string, provider string, nonce string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var exists bool
	err := a.db.QueryRowContext(ctx, queries.FLOW_CHECK_REPLAY_NONCE, tenantID, provider, nonce).Scan(&exists)
	if err != nil {
		return false, appErrors.NewInternalError("failed to check replay nonce: " + err.Error())
	}
	return exists, nil
}

package auth

import (
	"database/sql"

	audittrail "distributed-transaction-engine/src/features/audit-trail"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/features/authentication-authorization/infrastructure/adapters"
	"distributed-transaction-engine/src/features/authentication-authorization/infrastructure/ports"
	"distributed-transaction-engine/src/features/authentication-authorization/service"
	"distributed-transaction-engine/src/infra/observability"
)

type AuthenticationAuthorizationService = service.AuthenticationAuthorizationService
type ServiceDeps = service.ServiceDeps
type AuthRepositoryPort = ports.AuthRepositoryPort
type TokenVerifierPort = ports.TokenVerifierPort
type PrincipalVerifier = adapters.PrincipalVerifier

type AuthContext = contracts.AuthContext
type AuthorizeRequestCommand = contracts.AuthorizeRequestCommand
type AuthorizeResult = contracts.AuthorizeResult
type AuthenticateTokenCommand = contracts.AuthenticateTokenCommand
type AuthenticateAPIKeyCommand = contracts.AuthenticateAPIKeyCommand
type AuthenticateServiceCommand = contracts.AuthenticateServiceCommand
type AuthenticateWebhookCommand = contracts.AuthenticateWebhookCommand
type RegisterAPIKeyCommand = contracts.RegisterAPIKeyCommand
type RegisterAPIKeyResult = contracts.RegisterAPIKeyResult
type RegisterServiceIdentityCommand = contracts.RegisterServiceIdentityCommand
type RevokeCredentialCommand = contracts.RevokeCredentialCommand

type APIKeyRecord = contracts.APIKeyRecord
type ServiceIdentityRecord = contracts.ServiceIdentityRecord
type AuthDecisionRecord = contracts.AuthDecisionRecord
type RevocationRecord = contracts.RevocationRecord
type ProviderReplayRecord = contracts.ProviderReplayRecord

func NewService(deps ServiceDeps) *AuthenticationAuthorizationService {
	return service.NewAuthenticationAuthorizationService(deps)
}

func NewJWTTokenVerifier(verifier PrincipalVerifier) ports.TokenVerifierPort {
	return adapters.NewJWTTokenVerifierAdapter(verifier)
}

func NewMemoryRepository() ports.AuthRepositoryPort {
	return adapters.NewAuthMemoryRepositoryAdapter()
}

func NewPostgresRepository(db *sql.DB) ports.AuthRepositoryPort {
	return adapters.NewAuthPostgresRepositoryAdapter(db)
}

func NewDefaultService(repo ports.AuthRepositoryPort, auditSvc *audittrail.AuditTrailService) *AuthenticationAuthorizationService {
	return service.NewAuthenticationAuthorizationService(service.ServiceDeps{
		Repo:     repo,
		AuditSvc: auditSvc,
		Tracer:   observability.NewTracer("authentication-authorization-service"),
	})
}


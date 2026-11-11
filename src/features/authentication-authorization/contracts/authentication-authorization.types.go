package contracts

import (
	"time"

	"github.com/google/uuid"
)

type CallerType string

const (
	CallerTypeUserOIDC        CallerType = "USER_OIDC"
	CallerTypeAPIKey          CallerType = "API_KEY"
	CallerTypeInternalService CallerType = "INTERNAL_SERVICE"
	CallerTypeOperator        CallerType = "OPERATOR"
	CallerTypeProviderWebhook CallerType = "PROVIDER_WEBHOOK"
	CallerTypeKafkaProducer   CallerType = "KAFKA_PRODUCER"
)

type AuthDecision string

const (
	AuthDecisionAllow AuthDecision = "ALLOW"
	AuthDecisionDeny  AuthDecision = "DENY"
)

type RiskLevel string

const (
	RiskLevelLow      RiskLevel = "LOW"
	RiskLevelMedium   RiskLevel = "MEDIUM"
	RiskLevelHigh     RiskLevel = "HIGH"
	RiskLevelCritical RiskLevel = "CRITICAL"
)

type APIKeyStatus string

const (
	APIKeyStatusActive  APIKeyStatus = "ACTIVE"
	APIKeyStatusRevoked APIKeyStatus = "REVOKED"
	APIKeyStatusExpired APIKeyStatus = "EXPIRED"
)

type RevocationTargetType string

const (
	RevocationTargetTypeToken   RevocationTargetType = "TOKEN"
	RevocationTargetTypeUser    RevocationTargetType = "USER"
	RevocationTargetTypeKey     RevocationTargetType = "KEY"
	RevocationTargetTypeSession RevocationTargetType = "SESSION"
)

type AuthContext struct {
	CallerType     CallerType `json:"caller_type"`
	PrincipalID    string     `json:"principal_id"`
	TenantID       string     `json:"tenant_id"`
	Scopes         []string   `json:"scopes"`
	Roles          []string   `json:"roles"`
	SPIFFEID       string     `json:"spiffe_id,omitempty"`
	DelegatedUser  string     `json:"delegated_user,omitempty"`
	StepUpVerified bool       `json:"step_up_verified"`
	TokenJTI       string     `json:"token_jti,omitempty"`
}

type VerifiedToken struct {
	Subject   string
	TenantID  string
	Issuer    string
	Audience  []string
	TokenID   string
	Roles     []string
	Scopes    []string
	ExpiresAt time.Time
}

type AuthenticateTokenCommand struct {
	Token    string `json:"token"`
	Issuer   string `json:"issuer"`
	Audience string `json:"audience"`
}

type AuthenticateAPIKeyCommand struct {
	RawKey   string `json:"raw_key"`
	TenantID string `json:"tenant_id"`
	ClientIP string `json:"client_ip"`
}

type AuthenticateServiceCommand struct {
	SPIFFEID        string `json:"spiffe_id"`
	TrustDomain     string `json:"trust_domain"`
	Method          string `json:"method"`
	OnBehalfOfToken string `json:"on_behalf_of_token,omitempty"`
}

type AuthenticateWebhookCommand struct {
	Provider       string    `json:"provider"`
	TenantID       string    `json:"tenant_id"`
	RawPayload     []byte    `json:"-"`
	Signature      string    `json:"signature"`
	Timestamp      time.Time `json:"timestamp"`
	Nonce          string    `json:"nonce"`
	ProviderSecret []byte    `json:"-"`
}

type AuthorizeRequestCommand struct {
	Auth             AuthContext `json:"auth"`
	ResourceTenantID string      `json:"resource_tenant_id"`
	ResourceID       string      `json:"resource_id"`
	ResourceOwnerID  string      `json:"resource_owner_id"`
	Action           string      `json:"action"`
	SagaState        string      `json:"saga_state"`
	IsPostPivot      bool        `json:"is_post_pivot"`
	Amount           float64     `json:"amount"`
	ApproverID       string      `json:"approver_id,omitempty"`
}

type AuthorizeResult struct {
	Decision   AuthDecision `json:"decision"`
	HTTPStatus int          `json:"http_status"`
	ReasonCode string       `json:"reason_code"`
	Message    string       `json:"message"`
	AuditID    string       `json:"audit_id"`
}

type RegisterAPIKeyCommand struct {
	TenantID     string     `json:"tenant_id"`
	Name         string     `json:"name"`
	Scopes       []string   `json:"scopes"`
	AllowedCIDRs []string   `json:"allowed_cidrs"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
}

type RegisterAPIKeyResult struct {
	PublicID  string     `json:"public_id"`
	KeyPrefix string     `json:"key_prefix"`
	SecretKey string     `json:"secret_key"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type RegisterServiceIdentityCommand struct {
	TenantID       string   `json:"tenant_id"`
	SPIFFEID       string   `json:"spiffe_id"`
	TrustDomain    string   `json:"trust_domain"`
	AllowedMethods []string `json:"allowed_methods"`
}

type RevokeCredentialCommand struct {
	TenantID         string               `json:"tenant_id"`
	TargetType       RevocationTargetType `json:"target_type"`
	TargetID         string               `json:"target_id"`
	RevocationReason string               `json:"revocation_reason"`
	ExpiresAt        time.Time            `json:"expires_at"`
}

type APIKeyRecord struct {
	ID            uuid.UUID    `json:"id"`
	PublicID      string       `json:"public_id"`
	TenantID      string       `json:"tenant_id"`
	Source        string       `json:"source"`
	ExternalID    string       `json:"external_id"`
	CorrelationID uuid.UUID    `json:"correlation_id"`
	KeyPrefix     string       `json:"key_prefix"`
	KeyHash       string       `json:"key_hash"`
	Name          string       `json:"name"`
	Scopes        []string     `json:"scopes"`
	AllowedCIDRs  []string     `json:"allowed_cidrs"`
	Status        APIKeyStatus `json:"status"`
	ExpiresAt     *time.Time   `json:"expires_at,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
}

type ServiceIdentityRecord struct {
	ID             uuid.UUID `json:"id"`
	PublicID       string    `json:"public_id"`
	TenantID       string    `json:"tenant_id"`
	Source         string    `json:"source"`
	ExternalID     string    `json:"external_id"`
	CorrelationID  uuid.UUID `json:"correlation_id"`
	SPIFFEID       string    `json:"spiffe_id"`
	TrustDomain    string    `json:"trust_domain"`
	AllowedMethods []string  `json:"allowed_methods"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

type AuthDecisionRecord struct {
	ID             uuid.UUID    `json:"id"`
	PublicID       string       `json:"public_id"`
	TenantID       string       `json:"tenant_id"`
	Source         string       `json:"source"`
	ExternalID     string       `json:"external_id"`
	CorrelationID  uuid.UUID    `json:"correlation_id"`
	CallerType     CallerType   `json:"caller_type"`
	PrincipalID    string       `json:"principal_id"`
	Action         string       `json:"action"`
	ResourceID     string       `json:"resource_id"`
	Decision       AuthDecision `json:"decision"`
	ReasonCode     string       `json:"reason_code"`
	RiskLevel      RiskLevel    `json:"risk_level"`
	StepUpVerified bool         `json:"step_up_verified"`
	ApproverID     string       `json:"approver_id"`
	CreatedAt      time.Time    `json:"created_at"`
}

type RevocationRecord struct {
	ID               uuid.UUID            `json:"id"`
	PublicID         string               `json:"public_id"`
	TenantID         string               `json:"tenant_id"`
	Source           string               `json:"source"`
	ExternalID       string               `json:"external_id"`
	CorrelationID    uuid.UUID            `json:"correlation_id"`
	TargetType       RevocationTargetType `json:"target_type"`
	TargetID         string               `json:"target_id"`
	RevocationReason string               `json:"revocation_reason"`
	RevokedAt        time.Time            `json:"revoked_at"`
	ExpiresAt        time.Time            `json:"expires_at"`
	CreatedAt        time.Time            `json:"created_at"`
}

type ProviderReplayRecord struct {
	ID            uuid.UUID `json:"id"`
	PublicID      string    `json:"public_id"`
	TenantID      string    `json:"tenant_id"`
	Source        string    `json:"source"`
	ExternalID    string    `json:"external_id"`
	CorrelationID uuid.UUID `json:"correlation_id"`
	Provider      string    `json:"provider"`
	Nonce         string    `json:"nonce"`
	ReceivedAt    time.Time `json:"received_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}


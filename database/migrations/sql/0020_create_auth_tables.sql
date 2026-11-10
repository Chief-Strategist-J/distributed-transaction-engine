-- migration:      0020
-- description:    create api keys, service identities, auth decisions, revocation, and replay cache tables
-- author:         lead-architect
-- date:           2026-11-10T11:43:00Z
-- depends_on:     0019_create_audit_trail_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide multi-caller authentication, 5-layer authorization, credential revocation, and webhook replay protection

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'AUTH_ADMIN',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    key_prefix VARCHAR(16) NOT NULL,
    key_hash VARCHAR(64) NOT NULL,
    name VARCHAR(128) NOT NULL,
    scopes TEXT[] NOT NULL DEFAULT '{}',
    allowed_cidrs TEXT[] NOT NULL DEFAULT '{}',
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_api_keys PRIMARY KEY (tenant_id, id),
    CONSTRAINT api_keys_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT api_keys_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT api_keys_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT api_keys_prefix_uk UNIQUE (tenant_id, key_prefix),
    CONSTRAINT api_keys_public_id_ck CHECK (public_id ~ '^apk_[0-9a-zA-Z]{26}$'),
    CONSTRAINT api_keys_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT api_keys_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT api_keys_key_prefix_ck CHECK (key_prefix = btrim(key_prefix) AND length(key_prefix) >= 4),
    CONSTRAINT api_keys_key_hash_ck CHECK (length(key_hash) = 64),
    CONSTRAINT api_keys_status_ck CHECK (status IN ('ACTIVE', 'REVOKED', 'EXPIRED'))
);

CREATE TABLE IF NOT EXISTS service_identities (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'PLATFORM_MESH',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    spiffe_id VARCHAR(256) NOT NULL,
    trust_domain VARCHAR(128) NOT NULL,
    allowed_methods TEXT[] NOT NULL DEFAULT '{}',
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_service_identities PRIMARY KEY (tenant_id, id),
    CONSTRAINT service_identities_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT service_identities_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT service_identities_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT service_identities_spiffe_uk UNIQUE (tenant_id, spiffe_id),
    CONSTRAINT service_identities_public_id_ck CHECK (public_id ~ '^svi_[0-9a-zA-Z]{26}$'),
    CONSTRAINT service_identities_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT service_identities_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT service_identities_spiffe_id_ck CHECK (spiffe_id ~ '^spiffe://[a-zA-Z0-9.-]+/[a-zA-Z0-9._/-]+$'),
    CONSTRAINT service_identities_status_ck CHECK (status IN ('ACTIVE', 'SUSPENDED'))
);

CREATE TABLE IF NOT EXISTS auth_decision_audit (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'AUTH_ENGINE',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    caller_type VARCHAR(32) NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    action VARCHAR(128) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    decision VARCHAR(32) NOT NULL,
    reason_code VARCHAR(64) NOT NULL,
    risk_level VARCHAR(32) NOT NULL,
    step_up_verified BOOLEAN NOT NULL DEFAULT FALSE,
    approver_id VARCHAR(128) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_auth_decision_audit PRIMARY KEY (tenant_id, id),
    CONSTRAINT auth_decision_audit_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT auth_decision_audit_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT auth_decision_audit_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT auth_decision_audit_public_id_ck CHECK (public_id ~ '^ada_[0-9a-zA-Z]{26}$'),
    CONSTRAINT auth_decision_audit_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT auth_decision_audit_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT auth_decision_audit_decision_ck CHECK (decision IN ('ALLOW', 'DENY')),
    CONSTRAINT auth_decision_audit_risk_ck CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'))
);

CREATE TABLE IF NOT EXISTS revocation_records (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'AUTH_ADMIN',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    target_type VARCHAR(32) NOT NULL,
    target_id VARCHAR(128) NOT NULL,
    revocation_reason TEXT NOT NULL DEFAULT '',
    revoked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_revocation_records PRIMARY KEY (tenant_id, id),
    CONSTRAINT revocation_records_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT revocation_records_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT revocation_records_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT revocation_records_target_uk UNIQUE (tenant_id, target_type, target_id),
    CONSTRAINT revocation_records_public_id_ck CHECK (public_id ~ '^rev_[0-9a-zA-Z]{26}$'),
    CONSTRAINT revocation_records_target_type_ck CHECK (target_type IN ('TOKEN', 'USER', 'KEY', 'SESSION'))
);

CREATE TABLE IF NOT EXISTS provider_replay_cache (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'WEBHOOK_RECEIVER',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    provider VARCHAR(64) NOT NULL,
    nonce VARCHAR(128) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_provider_replay_cache PRIMARY KEY (tenant_id, id),
    CONSTRAINT provider_replay_cache_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT provider_replay_cache_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT provider_replay_cache_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT provider_replay_cache_nonce_uk UNIQUE (tenant_id, provider, nonce),
    CONSTRAINT provider_replay_cache_public_id_ck CHECK (public_id ~ '^prc_[0-9a-zA-Z]{26}$')
);

CREATE INDEX IF NOT EXISTS idx_api_keys_lookup
    ON api_keys (tenant_id, key_prefix, status);

CREATE INDEX IF NOT EXISTS idx_service_identities_lookup
    ON service_identities (tenant_id, spiffe_id, status);

CREATE INDEX IF NOT EXISTS idx_auth_decision_audit_principal
    ON auth_decision_audit (tenant_id, principal_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_revocation_records_lookup
    ON revocation_records (tenant_id, target_type, target_id, expires_at);

CREATE INDEX IF NOT EXISTS idx_provider_replay_cache_lookup
    ON provider_replay_cache (tenant_id, provider, nonce, expires_at);


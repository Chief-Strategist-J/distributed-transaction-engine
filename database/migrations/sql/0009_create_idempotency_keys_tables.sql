-- migration:      0009
-- description:    create authoritative transaction-scoped idempotency keys table
-- author:         lead-architect
-- date:           2026-11-09T14:02:00Z
-- depends_on:     0008_create_transactional_outbox_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce authoritative 4D idempotency scope, prevent duplicate execution, and store response snapshots

CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'API_GATEWAY',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    operation_id VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    request_fingerprint CHAR(64) NOT NULL,
    current_state VARCHAR(32) NOT NULL DEFAULT 'IN_PROGRESS',
    response_status INT,
    response_headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_body JSONB NOT NULL DEFAULT '{}'::jsonb,
    resource_reference VARCHAR(128) NOT NULL DEFAULT '',
    lease_owner VARCHAR(128) NOT NULL DEFAULT '',
    lease_until TIMESTAMPTZ,
    recovery_fencing_token BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_idempotency_keys PRIMARY KEY (tenant_id, id),
    CONSTRAINT idempotency_keys_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT idempotency_keys_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT idempotency_keys_scope_uk UNIQUE (tenant_id, principal_id, operation_id, idempotency_key),
    CONSTRAINT idempotency_keys_public_id_ck CHECK (public_id ~ '^idk_[0-9a-zA-Z]{26}$'),
    CONSTRAINT idempotency_keys_state_ck CHECK (current_state IN ('IN_PROGRESS', 'COMPLETED', 'REJECTED_FINAL', 'UNKNOWN')),
    CONSTRAINT idempotency_keys_fingerprint_ck CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT idempotency_keys_status_ck CHECK (response_status IS NULL OR (response_status >= 100 AND response_status <= 599)),
    CONSTRAINT idempotency_keys_principal_ck CHECK (principal_id = btrim(principal_id) AND principal_id <> ''),
    CONSTRAINT idempotency_keys_operation_ck CHECK (operation_id = btrim(operation_id) AND operation_id <> ''),
    CONSTRAINT idempotency_keys_key_ck CHECK (idempotency_key = btrim(idempotency_key) AND idempotency_key <> '')
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at 
    ON idempotency_keys (tenant_id, expires_at);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_leases 
    ON idempotency_keys (tenant_id, lease_until) 
    WHERE current_state = 'IN_PROGRESS';


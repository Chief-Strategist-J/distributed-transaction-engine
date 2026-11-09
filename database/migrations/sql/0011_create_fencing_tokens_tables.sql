-- migration:      0011
-- description:    create fencing leases and fencing violation audit tables
-- author:         lead-architect
-- date:           2026-11-09T14:45:00Z
-- depends_on:     0010_create_retry_policies_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide atomic monotonically increasing lease epochs and zombie write suppression barriers

CREATE TABLE IF NOT EXISTS fencing_leases (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_LEASING',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    holder_id VARCHAR(128) NOT NULL,
    epoch BIGINT NOT NULL DEFAULT 1,
    lease_duration_ms INT NOT NULL DEFAULT 15000,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_fencing_leases PRIMARY KEY (tenant_id, id),
    CONSTRAINT fencing_leases_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT fencing_leases_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT fencing_leases_resource_uk UNIQUE (tenant_id, resource_id),
    CONSTRAINT fencing_leases_natural_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT fencing_leases_public_id_ck CHECK (public_id ~ '^fnl_[0-9a-zA-Z]{26}$'),
    CONSTRAINT fencing_leases_resource_ck CHECK (resource_id = btrim(resource_id) AND resource_id <> ''),
    CONSTRAINT fencing_leases_holder_ck CHECK (holder_id = btrim(holder_id) AND holder_id <> ''),
    CONSTRAINT fencing_leases_epoch_ck CHECK (epoch >= 1),
    CONSTRAINT fencing_leases_duration_ck CHECK (lease_duration_ms >= 1000)
);

CREATE INDEX IF NOT EXISTS idx_fencing_leases_expiry 
    ON fencing_leases (tenant_id, expires_at) 
    WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS fencing_audits (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    attempted_token BIGINT NOT NULL,
    current_token BIGINT NOT NULL,
    rejected_holder VARCHAR(128) NOT NULL,
    violation_type VARCHAR(64) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_fencing_audits PRIMARY KEY (tenant_id, id),
    CONSTRAINT fencing_audits_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT fencing_audits_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT fencing_audits_public_id_ck CHECK (public_id ~ '^fnv_[0-9a-zA-Z]{26}$'),
    CONSTRAINT fencing_audits_type_ck CHECK (violation_type IN ('STALE_TOKEN', 'LEASE_EXPIRED', 'UNAUTHORIZED_HOLDER'))
);

CREATE INDEX IF NOT EXISTS idx_fencing_audits_resource 
    ON fencing_audits (tenant_id, resource_id, recorded_at DESC);


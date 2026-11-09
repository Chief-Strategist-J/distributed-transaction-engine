-- migration:      0012
-- description:    create compensation retry journals and step pause tables with strict constraints
-- author:         lead-architect
-- date:           2026-11-09T15:25:00Z
-- depends_on:     0011_create_fencing_tokens_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide time-horizon compensation retries, ambiguity verification, and pre-deadline escalation

CREATE TABLE IF NOT EXISTS compensation_retry_transitions (
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_compensation_retry_transitions PRIMARY KEY (from_state, to_state)
);

INSERT INTO compensation_retry_transitions (from_state, to_state) VALUES
    ('PENDING', 'RETRY_SCHEDULED'),
    ('PENDING', 'VERIFYING_AMBIGUOUS'),
    ('PENDING', 'NEEDS_ATTENTION'),
    ('PENDING', 'STEP_TYPE_PAUSED'),
    ('PENDING', 'FROZEN_BREAKER'),
    ('RETRY_SCHEDULED', 'RETRY_SCHEDULED'),
    ('RETRY_SCHEDULED', 'VERIFYING_AMBIGUOUS'),
    ('RETRY_SCHEDULED', 'COMPENSATED'),
    ('RETRY_SCHEDULED', 'NEEDS_ATTENTION'),
    ('RETRY_SCHEDULED', 'STEP_TYPE_PAUSED'),
    ('RETRY_SCHEDULED', 'FROZEN_BREAKER'),
    ('VERIFYING_AMBIGUOUS', 'COMPENSATED'),
    ('VERIFYING_AMBIGUOUS', 'RETRY_SCHEDULED'),
    ('VERIFYING_AMBIGUOUS', 'NEEDS_ATTENTION'),
    ('STEP_TYPE_PAUSED', 'RETRY_SCHEDULED'),
    ('FROZEN_BREAKER', 'RETRY_SCHEDULED'),
    ('NEEDS_ATTENTION', 'RETRY_SCHEDULED'),
    ('NEEDS_ATTENTION', 'COMPENSATED')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS compensation_retries (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_COMPENSATION',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    step_id VARCHAR(128) NOT NULL,
    step_type VARCHAR(64) NOT NULL,
    dependency_id VARCHAR(64) NOT NULL,
    attempt_number INT NOT NULL DEFAULT 1,
    max_attempts INT NOT NULL DEFAULT 10,
    first_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    business_deadline TIMESTAMPTZ NOT NULL,
    next_retry_at TIMESTAMPTZ,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    last_error TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_compensation_retries PRIMARY KEY (tenant_id, id),
    CONSTRAINT compensation_retries_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT compensation_retries_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT compensation_retries_step_uk UNIQUE (tenant_id, saga_id, step_id),
    CONSTRAINT compensation_retries_natural_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT compensation_retries_public_id_ck CHECK (public_id ~ '^cpr_[0-9a-zA-Z]{26}$'),
    CONSTRAINT compensation_retries_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT compensation_retries_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT compensation_retries_step_id_ck CHECK (step_id = btrim(step_id) AND step_id <> ''),
    CONSTRAINT compensation_retries_step_type_ck CHECK (step_type = btrim(step_type) AND step_type <> ''),
    CONSTRAINT compensation_retries_dependency_ck CHECK (dependency_id = btrim(dependency_id) AND dependency_id <> ''),
    CONSTRAINT compensation_retries_attempt_ck CHECK (attempt_number >= 1),
    CONSTRAINT compensation_retries_max_attempts_ck CHECK (max_attempts >= 1 AND max_attempts <= 100),
    CONSTRAINT compensation_retries_version_ck CHECK (version >= 1),
    CONSTRAINT compensation_retries_status_ck CHECK (status IN (
        'PENDING', 'RETRY_SCHEDULED', 'VERIFYING_AMBIGUOUS',
        'COMPENSATED', 'NEEDS_ATTENTION', 'STEP_TYPE_PAUSED', 'FROZEN_BREAKER'
    ))
);

CREATE TABLE IF NOT EXISTS compensation_step_type_pauses (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_COMPENSATION',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    step_type VARCHAR(64) NOT NULL,
    is_paused BOOLEAN NOT NULL DEFAULT TRUE,
    reason TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_compensation_step_type_pauses PRIMARY KEY (tenant_id, id),
    CONSTRAINT compensation_step_type_pauses_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT compensation_step_type_pauses_step_type_uk UNIQUE (tenant_id, step_type),
    CONSTRAINT compensation_step_type_pauses_natural_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT compensation_step_type_pauses_public_id_ck CHECK (public_id ~ '^stp_[0-9a-zA-Z]{26}$'),
    CONSTRAINT compensation_step_type_pauses_step_type_ck CHECK (step_type = btrim(step_type) AND step_type <> ''),
    CONSTRAINT compensation_step_type_pauses_version_ck CHECK (version >= 1)
);

CREATE INDEX IF NOT EXISTS idx_compensation_retries_next_retry 
    ON compensation_retries (tenant_id, next_retry_at) 
    WHERE status = 'RETRY_SCHEDULED';

CREATE INDEX IF NOT EXISTS idx_compensation_retries_deadline 
    ON compensation_retries (tenant_id, business_deadline) 
    WHERE status IN ('PENDING', 'RETRY_SCHEDULED', 'VERIFYING_AMBIGUOUS');

CREATE INDEX IF NOT EXISTS idx_compensation_step_type_pauses_active
    ON compensation_step_type_pauses (tenant_id, step_type)
    WHERE is_paused = TRUE;

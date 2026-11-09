-- migration:      0010
-- description:    create retry policies and step retry execution journals
-- author:         lead-architect
-- date:           2026-11-09T14:22:00Z
-- depends_on:     0009_create_idempotency_keys_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide declarative backoff policies, circuit breaker integration, and step retry tracking

CREATE TABLE IF NOT EXISTS retry_policies (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_CONFIG',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    policy_name VARCHAR(128) NOT NULL,
    max_attempts INT NOT NULL DEFAULT 5,
    max_elapsed_seconds INT NOT NULL DEFAULT 300,
    base_interval_millis INT NOT NULL DEFAULT 200,
    max_interval_millis INT NOT NULL DEFAULT 30000,
    backoff_algorithm VARCHAR(32) NOT NULL DEFAULT 'EXPONENTIAL_FULL_JITTER',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_retry_policies PRIMARY KEY (tenant_id, id),
    CONSTRAINT retry_policies_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT retry_policies_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT retry_policies_name_uk UNIQUE (tenant_id, policy_name),
    CONSTRAINT retry_policies_public_id_ck CHECK (public_id ~ '^rtp_[0-9a-zA-Z]{26}$'),
    CONSTRAINT retry_policies_name_ck CHECK (policy_name = btrim(policy_name) AND policy_name <> ''),
    CONSTRAINT retry_policies_max_attempts_ck CHECK (max_attempts >= 1 AND max_attempts <= 100),
    CONSTRAINT retry_policies_max_elapsed_ck CHECK (max_elapsed_seconds >= 1),
    CONSTRAINT retry_policies_base_ck CHECK (base_interval_millis >= 10),
    CONSTRAINT retry_policies_max_interval_ck CHECK (max_interval_millis >= base_interval_millis),
    CONSTRAINT retry_policies_algo_ck CHECK (backoff_algorithm IN ('EXPONENTIAL_FULL_JITTER', 'EXPONENTIAL_EQUAL_JITTER', 'LINEAR_BACKOFF'))
);

CREATE TABLE IF NOT EXISTS step_retries (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    step_id VARCHAR(128) NOT NULL,
    attempt_number INT NOT NULL,
    error_class VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL DEFAULT '',
    error_fingerprint CHAR(64) NOT NULL DEFAULT '',
    retry_after_seconds INT NOT NULL DEFAULT 0,
    computed_delay_millis INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_step_retries PRIMARY KEY (tenant_id, id),
    CONSTRAINT step_retries_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT step_retries_attempt_uk UNIQUE (tenant_id, saga_id, step_id, attempt_number),
    CONSTRAINT step_retries_public_id_ck CHECK (public_id ~ '^sre_[0-9a-zA-Z]{26}$'),
    CONSTRAINT step_retries_attempt_ck CHECK (attempt_number >= 1)
);

CREATE INDEX IF NOT EXISTS idx_step_retries_saga_step 
    ON step_retries (tenant_id, saga_id, step_id);

CREATE INDEX IF NOT EXISTS idx_step_retries_next_attempt 
    ON step_retries (tenant_id, next_attempt_at) 
    WHERE next_attempt_at IS NOT NULL;


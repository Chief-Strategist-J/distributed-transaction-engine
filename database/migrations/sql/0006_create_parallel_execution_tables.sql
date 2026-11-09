-- migration:      0006
-- description:    create parallel execution group and branch tables
-- author:         lead-architect
-- date:           2026-11-09T13:11:00Z
-- depends_on:     0005_create_sequential_execution_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         support fork/join parallel saga execution, branch tracking, and quorum evaluation

CREATE TABLE IF NOT EXISTS parallel_groups (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    join_policy VARCHAR(32) NOT NULL,
    quorum_count INT NOT NULL DEFAULT 0,
    current_state VARCHAR(32) NOT NULL DEFAULT 'FANOUT_ACTIVE',
    total_branches INT NOT NULL DEFAULT 0,
    succeeded_branches INT NOT NULL DEFAULT 0,
    failed_branches INT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    correlation_id UUID NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_parallel_groups PRIMARY KEY (tenant_id, id),
    CONSTRAINT parallel_groups_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT parallel_groups_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT parallel_groups_tenant_saga_ext_uk UNIQUE (tenant_id, saga_id, external_id),
    CONSTRAINT parallel_groups_public_id_ck CHECK (public_id ~ '^par_[0-9a-zA-Z]{26}$'),
    CONSTRAINT parallel_groups_join_policy_ck CHECK (join_policy IN ('ALL_REQUIRED', 'FIRST_SUCCESS', 'N_OF_M', 'ALL_SETTLED')),
    CONSTRAINT parallel_groups_state_ck CHECK (current_state IN ('FANOUT_ACTIVE', 'JOIN_SUCCEEDED', 'JOIN_FAILED', 'COMPENSATING', 'COMPENSATED')),
    CONSTRAINT parallel_groups_version_ck CHECK (version >= 1),
    CONSTRAINT fk_parallel_groups_saga FOREIGN KEY (tenant_id, saga_id)
        REFERENCES saga_instances (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS parallel_branches (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    group_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    branch_id VARCHAR(64) NOT NULL,
    branch_name VARCHAR(128) NOT NULL,
    role VARCHAR(32) NOT NULL,
    effect_type VARCHAR(32) NOT NULL,
    current_state VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    idempotency_key VARCHAR(128) NOT NULL,
    branch_input JSONB NOT NULL DEFAULT '{}'::jsonb,
    branch_output JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message VARCHAR(512) NOT NULL DEFAULT '',
    correlation_id UUID NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    dispatched_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_parallel_branches PRIMARY KEY (tenant_id, id),
    CONSTRAINT parallel_branches_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT parallel_branches_group_branch_uk UNIQUE (tenant_id, group_id, branch_id),
    CONSTRAINT parallel_branches_idempotency_uk UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT parallel_branches_public_id_ck CHECK (public_id ~ '^pbr_[0-9a-zA-Z]{26}$'),
    CONSTRAINT parallel_branches_role_ck CHECK (role IN ('REQUIRED', 'OPTIONAL', 'ALTERNATIVE')),
    CONSTRAINT parallel_branches_effect_ck CHECK (effect_type IN ('HOLD', 'COMPENSABLE')),
    CONSTRAINT parallel_branches_state_ck CHECK (current_state IN ('PENDING', 'DISPATCHED', 'SUCCEEDED', 'REJECTED', 'FAILED_RETRYABLE', 'UNKNOWN', 'NOT_EXECUTED', 'CANCEL_REQUESTED', 'CANCELLED', 'COMPENSATING', 'COMPENSATED')),
    CONSTRAINT fk_parallel_branches_group FOREIGN KEY (tenant_id, group_id)
        REFERENCES parallel_groups (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_parallel_groups_saga ON parallel_groups (tenant_id, saga_id, current_state);
CREATE INDEX IF NOT EXISTS idx_parallel_branches_group ON parallel_branches (tenant_id, group_id, current_state);

-- migration:      0003
-- description:    create saga instances and step execution journals
-- author:         lead-architect
-- date:           2026-11-03T11:40:00Z
-- depends_on:     0002_create_workflow_definitions_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         runtime execution journal for saga orchestration feature

CREATE TABLE IF NOT EXISTS saga_instances (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    workflow_definition_id UUID NOT NULL,
    workflow_version BIGINT NOT NULL,
    spec_hash CHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    current_state VARCHAR(32) NOT NULL DEFAULT 'CREATED',
    version BIGINT NOT NULL DEFAULT 1,
    fencing_token BIGINT NOT NULL DEFAULT 1,
    initial_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    final_output JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_saga_instances PRIMARY KEY (tenant_id, id),
    CONSTRAINT saga_instances_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT saga_instances_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT saga_instances_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT saga_instances_public_id_ck CHECK (public_id ~ '^sga_[0-9a-zA-Z]{26}$'),
    CONSTRAINT saga_instances_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT saga_instances_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT saga_instances_version_ck CHECK (version >= 1),
    CONSTRAINT saga_instances_fencing_token_ck CHECK (fencing_token >= 1),
    CONSTRAINT saga_instances_state_ck CHECK (current_state IN ('CREATED', 'RUNNING', 'COMPENSATING', 'COMPLETED', 'FAILED')),
    CONSTRAINT fk_saga_instances_workflow FOREIGN KEY (tenant_id, workflow_definition_id)
        REFERENCES workflow_definitions (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS saga_step_executions (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    execution_state VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    step_input JSONB NOT NULL DEFAULT '{}'::jsonb,
    step_output JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message VARCHAR(512) NOT NULL DEFAULT '',
    idempotency_key VARCHAR(128) NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_saga_step_executions PRIMARY KEY (tenant_id, id),
    CONSTRAINT saga_step_executions_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT saga_step_executions_saga_step_uk UNIQUE (tenant_id, saga_id, step_id),
    CONSTRAINT saga_step_executions_idempotency_uk UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT saga_step_executions_public_id_ck CHECK (public_id ~ '^stx_[0-9a-zA-Z]{26}$'),
    CONSTRAINT saga_step_executions_step_id_ck CHECK (step_id = btrim(step_id) AND step_id <> ''),
    CONSTRAINT saga_step_executions_attempts_ck CHECK (attempt_count >= 0),
    CONSTRAINT saga_step_executions_state_ck CHECK (execution_state IN ('PENDING', 'DISPATCHED', 'SUCCEEDED', 'FAILED', 'COMPENSATING', 'COMPENSATED', 'SKIPPED')),
    CONSTRAINT fk_saga_step_executions_saga FOREIGN KEY (tenant_id, saga_id)
        REFERENCES saga_instances (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_saga_instances_state ON saga_instances (tenant_id, current_state);
CREATE INDEX IF NOT EXISTS idx_saga_instances_workflow ON saga_instances (tenant_id, workflow_definition_id, workflow_version);
CREATE INDEX IF NOT EXISTS idx_saga_step_executions_state ON saga_step_executions (tenant_id, saga_id, execution_state);


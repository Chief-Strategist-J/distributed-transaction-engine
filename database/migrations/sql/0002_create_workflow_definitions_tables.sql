-- migration:      0002
-- description:    create workflow definitions and step topology tables
-- author:         lead-architect
-- date:           2026-11-03T10:00:00Z
-- depends_on:     0001_create_saga_state_machine_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         schema for workflow-definitions feature

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    name VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    spec_hash CHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    raw_spec JSONB NOT NULL,
    total_steps INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_workflow_definitions PRIMARY KEY (tenant_id, id),
    CONSTRAINT workflow_definitions_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT workflow_definitions_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT workflow_definitions_tenant_name_ver_uk UNIQUE (tenant_id, name, version),
    CONSTRAINT workflow_definitions_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT workflow_definitions_spec_hash_uk UNIQUE (tenant_id, name, spec_hash),
    CONSTRAINT workflow_definitions_public_id_ck CHECK (public_id ~ '^wfd_[0-9a-zA-Z]{26}$'),
    CONSTRAINT workflow_definitions_name_ck CHECK (name = btrim(name) AND name <> ''),
    CONSTRAINT workflow_definitions_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT workflow_definitions_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT workflow_definitions_version_ck CHECK (version >= 1),
    CONSTRAINT workflow_definitions_status_ck CHECK (status IN ('ACTIVE', 'DEPRECATED', 'ARCHIVED')),
    CONSTRAINT workflow_definitions_total_steps_ck CHECK (total_steps >= 1),
    CONSTRAINT workflow_definitions_raw_spec_ck CHECK (jsonb_typeof(raw_spec) = 'object')
);

CREATE TABLE IF NOT EXISTS workflow_definition_steps (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    workflow_id UUID NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    is_pivot BOOLEAN NOT NULL DEFAULT FALSE,
    action_type VARCHAR(64) NOT NULL,
    action_endpoint VARCHAR(256) NOT NULL,
    compensation_action VARCHAR(64) NOT NULL DEFAULT '',
    compensation_endpoint VARCHAR(256) NOT NULL DEFAULT '',
    timeout_seconds INT NOT NULL DEFAULT 30,
    max_retries INT NOT NULL DEFAULT 3,
    dependencies JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_workflow_definition_steps PRIMARY KEY (tenant_id, id),
    CONSTRAINT workflow_definition_steps_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT workflow_definition_steps_workflow_step_uk UNIQUE (tenant_id, workflow_id, step_id),
    CONSTRAINT workflow_definition_steps_public_id_ck CHECK (public_id ~ '^stp_[0-9a-zA-Z]{26}$'),
    CONSTRAINT workflow_definition_steps_step_id_ck CHECK (step_id = btrim(step_id) AND step_id <> ''),
    CONSTRAINT workflow_definition_steps_step_name_ck CHECK (step_name = btrim(step_name) AND step_name <> ''),
    CONSTRAINT workflow_definition_steps_timeout_ck CHECK (timeout_seconds >= 1 AND timeout_seconds <= 86400),
    CONSTRAINT workflow_definition_steps_retries_ck CHECK (max_retries >= 0 AND max_retries <= 100),
    CONSTRAINT workflow_definition_steps_dependencies_ck CHECK (jsonb_typeof(dependencies) = 'array'),
    CONSTRAINT workflow_definition_steps_pivot_comp_ck CHECK (
        (is_pivot = TRUE AND compensation_action = '') OR
        (is_pivot = FALSE AND compensation_action <> '')
    ),
    CONSTRAINT fk_workflow_definition_steps_workflow FOREIGN KEY (tenant_id, workflow_id)
        REFERENCES workflow_definitions (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_workflow_definitions_lookup
    ON workflow_definitions (tenant_id, name, version);

CREATE INDEX IF NOT EXISTS idx_workflow_definitions_public
    ON workflow_definitions (tenant_id, public_id);

CREATE INDEX IF NOT EXISTS idx_workflow_definition_steps_wf
    ON workflow_definition_steps (tenant_id, workflow_id);


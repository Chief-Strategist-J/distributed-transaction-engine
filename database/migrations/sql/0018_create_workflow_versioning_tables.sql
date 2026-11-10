-- migration:      0018
-- description:    create workflow versioning, promotions, cohorts, and migrations tables
-- author:         lead-architect
-- date:           2026-11-10T10:48:00Z
-- depends_on:     0017_create_scheduled_execution_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce invariant pinning, maker-checker promotion, canary cohorts, and safe-point migrations

CREATE TABLE IF NOT EXISTS workflow_bundles (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    workflow_name VARCHAR(128) NOT NULL,
    version_tag VARCHAR(64) NOT NULL,
    content_hash CHAR(64) NOT NULL,
    signature TEXT NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT',
    bundle_payload JSONB NOT NULL,
    capabilities_required JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_workflow_bundles PRIMARY KEY (tenant_id, id),
    CONSTRAINT workflow_bundles_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT workflow_bundles_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT workflow_bundles_tenant_name_ver_uk UNIQUE (tenant_id, workflow_name, version_tag),
    CONSTRAINT workflow_bundles_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT workflow_bundles_content_hash_uk UNIQUE (tenant_id, workflow_name, content_hash),
    CONSTRAINT workflow_bundles_public_id_ck CHECK (public_id ~ '^wvb_[0-9a-zA-Z]{26}$'),
    CONSTRAINT workflow_bundles_workflow_name_ck CHECK (workflow_name = btrim(workflow_name) AND workflow_name <> ''),
    CONSTRAINT workflow_bundles_version_tag_ck CHECK (version_tag = btrim(version_tag) AND version_tag <> ''),
    CONSTRAINT workflow_bundles_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT workflow_bundles_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT workflow_bundles_status_ck CHECK (status IN ('DRAFT', 'VALIDATED', 'ACTIVE', 'DEPRECATED', 'BLOCKED', 'ARCHIVED')),
    CONSTRAINT workflow_bundles_payload_ck CHECK (jsonb_typeof(bundle_payload) = 'object'),
    CONSTRAINT workflow_bundles_capabilities_ck CHECK (jsonb_typeof(capabilities_required) = 'array')
);

CREATE TABLE IF NOT EXISTS workflow_version_promotions (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    bundle_id UUID NOT NULL,
    action VARCHAR(32) NOT NULL,
    proposer_id VARCHAR(64) NOT NULL,
    approver_id VARCHAR(64) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    reason TEXT NOT NULL DEFAULT '',
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_workflow_version_promotions PRIMARY KEY (tenant_id, id),
    CONSTRAINT workflow_version_promotions_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT workflow_version_promotions_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT workflow_version_promotions_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT workflow_version_promotions_public_id_ck CHECK (public_id ~ '^wvp_[0-9a-zA-Z]{26}$'),
    CONSTRAINT workflow_version_promotions_action_ck CHECK (action IN ('PUBLISH', 'PROMOTE', 'DEPRECATE', 'BLOCK', 'ARCHIVE')),
    CONSTRAINT workflow_version_promotions_status_ck CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
    CONSTRAINT workflow_version_promotions_proposer_ck CHECK (proposer_id = btrim(proposer_id) AND proposer_id <> ''),
    CONSTRAINT workflow_version_promotions_dual_auth_ck CHECK (
        (status = 'APPROVED' AND approver_id <> '' AND approver_id <> proposer_id) OR
        (status <> 'APPROVED')
    ),
    CONSTRAINT fk_workflow_version_promotions_bundle FOREIGN KEY (tenant_id, bundle_id)
        REFERENCES workflow_bundles (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS workflow_entity_cohorts (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    workflow_name VARCHAR(128) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    cohort_bucket INT NOT NULL,
    bundle_id UUID NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_workflow_entity_cohorts PRIMARY KEY (tenant_id, id),
    CONSTRAINT workflow_entity_cohorts_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT workflow_entity_cohorts_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT workflow_entity_cohorts_entity_uk UNIQUE (tenant_id, workflow_name, entity_id),
    CONSTRAINT workflow_entity_cohorts_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT workflow_entity_cohorts_public_id_ck CHECK (public_id ~ '^wvc_[0-9a-zA-Z]{26}$'),
    CONSTRAINT workflow_entity_cohorts_entity_id_ck CHECK (entity_id = btrim(entity_id) AND entity_id <> ''),
    CONSTRAINT workflow_entity_cohorts_bucket_ck CHECK (cohort_bucket >= 0 AND cohort_bucket <= 100),
    CONSTRAINT fk_workflow_entity_cohorts_bundle FOREIGN KEY (tenant_id, bundle_id)
        REFERENCES workflow_bundles (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS workflow_version_migrations (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    from_bundle_id UUID NOT NULL,
    to_bundle_id UUID NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    safe_point_step_id VARCHAR(64) NOT NULL,
    state_mapping_applied JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_workflow_version_migrations PRIMARY KEY (tenant_id, id),
    CONSTRAINT workflow_version_migrations_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT workflow_version_migrations_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT workflow_version_migrations_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT workflow_version_migrations_public_id_ck CHECK (public_id ~ '^wvm_[0-9a-zA-Z]{26}$'),
    CONSTRAINT workflow_version_migrations_status_ck CHECK (status IN ('PENDING', 'EXECUTED', 'ROLLED_BACK', 'FAILED')),
    CONSTRAINT workflow_version_migrations_mapping_ck CHECK (jsonb_typeof(state_mapping_applied) = 'object'),
    CONSTRAINT fk_workflow_version_migrations_from FOREIGN KEY (tenant_id, from_bundle_id)
        REFERENCES workflow_bundles (tenant_id, id) ON DELETE RESTRICT,
    CONSTRAINT fk_workflow_version_migrations_to FOREIGN KEY (tenant_id, to_bundle_id)
        REFERENCES workflow_bundles (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_workflow_bundles_lookup
    ON workflow_bundles (tenant_id, workflow_name, status);

CREATE INDEX IF NOT EXISTS idx_workflow_version_promotions_bundle
    ON workflow_version_promotions (tenant_id, bundle_id, status);

CREATE INDEX IF NOT EXISTS idx_workflow_entity_cohorts_lookup
    ON workflow_entity_cohorts (tenant_id, workflow_name, entity_id);

CREATE INDEX IF NOT EXISTS idx_workflow_version_migrations_saga
    ON workflow_version_migrations (tenant_id, saga_id, status);


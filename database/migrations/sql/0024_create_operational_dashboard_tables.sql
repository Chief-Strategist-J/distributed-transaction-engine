-- migration:      0024
-- description:    create operational dashboard stream sessions, tile snapshots, action previews, and audit events
-- author:         lead-architect
-- date:           2026-11-10T14:30:00Z
-- depends_on:     0023_create_transaction_query_api_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide authoritative snapshot tiles, scoped SSE streaming, snapshot-bound action previews, and operator audit logging

CREATE TABLE IF NOT EXISTS dashboard_stream_sessions (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'DASHBOARD_GATEWAY',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    scope_type VARCHAR(32) NOT NULL,
    impersonation_ticket VARCHAR(128),
    impersonation_expires_at TIMESTAMPTZ,
    reauth_deadline TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT pk_dashboard_stream_sessions PRIMARY KEY (tenant_id, id),
    CONSTRAINT dashboard_stream_sessions_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT dashboard_stream_sessions_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT dashboard_stream_sessions_public_id_ck CHECK (public_id ~ '^dss_[0-9a-zA-Z]{26}$'),
    CONSTRAINT dashboard_stream_sessions_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT dashboard_stream_sessions_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT dashboard_stream_sessions_scope_ck CHECK (scope_type IN ('TENANT_ADMIN', 'SUPPORT', 'OPERATOR')),
    CONSTRAINT dashboard_stream_sessions_status_ck CHECK (status IN ('CONNECTING', 'ACTIVE', 'STALE_PAUSED', 'REVOKED', 'TERMINATED'))
);

CREATE TABLE IF NOT EXISTS dashboard_tile_snapshots (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'METRICS_ENGINE',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    tile_key VARCHAR(64) NOT NULL,
    metrics_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_age_ms BIGINT NOT NULL DEFAULT 0,
    is_stale BOOLEAN NOT NULL DEFAULT FALSE,
    as_of_version BIGINT NOT NULL DEFAULT 1,
    source_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT pk_dashboard_tile_snapshots PRIMARY KEY (tenant_id, id),
    CONSTRAINT dashboard_tile_snapshots_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT dashboard_tile_snapshots_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT dashboard_tile_snapshots_public_id_ck CHECK (public_id ~ '^dts_[0-9a-zA-Z]{26}$'),
    CONSTRAINT dashboard_tile_snapshots_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT dashboard_tile_snapshots_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT dashboard_tile_snapshots_tile_key_ck CHECK (tile_key IN ('sagas_by_state', 'outcome_rates', 'unknown_outcomes', 'dlq_metrics', 'latency_quantiles', 'pipeline_health', 'compensations'))
);

CREATE TABLE IF NOT EXISTS dashboard_action_previews (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'INTERVENTION_ENGINE',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id VARCHAR(128) NOT NULL,
    action_type VARCHAR(32) NOT NULL,
    snapshot_version BIGINT NOT NULL,
    requires_second_approver BOOLEAN NOT NULL DEFAULT FALSE,
    money_at_stake NUMERIC(18,4) NOT NULL DEFAULT 0.0000,
    preview_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING_CONFIRMATION',
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT pk_dashboard_action_previews PRIMARY KEY (tenant_id, id),
    CONSTRAINT dashboard_action_previews_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT dashboard_action_previews_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT dashboard_action_previews_public_id_ck CHECK (public_id ~ '^dap_[0-9a-zA-Z]{26}$'),
    CONSTRAINT dashboard_action_previews_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT dashboard_action_previews_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT dashboard_action_previews_action_ck CHECK (action_type IN ('PAUSE', 'RESUME', 'RETRY_STEP', 'FORCE_COMPENSATE', 'MARK_COMPLETED', 'SKIP_STEP')),
    CONSTRAINT dashboard_action_previews_status_ck CHECK (status IN ('PENDING_CONFIRMATION', 'CONFIRMED', 'EXPIRED', 'MISMATCH_REJECTED'))
);

CREATE TABLE IF NOT EXISTS dashboard_audit_events (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'DASHBOARD_AUDIT',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    operator_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    ip_address VARCHAR(45) NOT NULL DEFAULT '127.0.0.1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT pk_dashboard_audit_events PRIMARY KEY (tenant_id, id),
    CONSTRAINT dashboard_audit_events_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT dashboard_audit_events_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT dashboard_audit_events_public_id_ck CHECK (public_id ~ '^dae_[0-9a-zA-Z]{26}$'),
    CONSTRAINT dashboard_audit_events_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT dashboard_audit_events_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> '')
);

CREATE INDEX IF NOT EXISTS idx_dashboard_stream_sessions_tenant ON dashboard_stream_sessions (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_dashboard_tile_snapshots_tenant ON dashboard_tile_snapshots (tenant_id, tile_key);
CREATE INDEX IF NOT EXISTS idx_dashboard_action_previews_saga ON dashboard_action_previews (tenant_id, saga_id, status);
CREATE INDEX IF NOT EXISTS idx_dashboard_audit_events_operator ON dashboard_audit_events (tenant_id, operator_id, created_at);


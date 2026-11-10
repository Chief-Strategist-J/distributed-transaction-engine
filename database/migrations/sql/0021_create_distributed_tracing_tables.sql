-- migration:      0021
-- description:    create distributed tracing deep sessions, sampling rules, spans buffer, and async links registry
-- author:         lead-architect
-- date:           2026-11-10T12:02:00Z
-- depends_on:     0020_create_auth_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide W3C context propagation, asynchronous boundary linking, adaptive tail sampling, and LEVEL E deep tracing sessions

CREATE TABLE IF NOT EXISTS deep_trace_sessions (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SRE_OPS',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    target_type VARCHAR(32) NOT NULL,
    target_value VARCHAR(128) NOT NULL,
    sampling_rate DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    max_spans INT NOT NULL DEFAULT 1000,
    expires_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_deep_trace_sessions PRIMARY KEY (tenant_id, id),
    CONSTRAINT deep_trace_sessions_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT deep_trace_sessions_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT deep_trace_sessions_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT deep_trace_sessions_target_uk UNIQUE (tenant_id, target_type, target_value),
    CONSTRAINT deep_trace_sessions_public_id_ck CHECK (public_id ~ '^dts_[0-9a-zA-Z]{26}$'),
    CONSTRAINT deep_trace_sessions_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT deep_trace_sessions_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT deep_trace_sessions_target_type_ck CHECK (target_type IN ('SAGA_ID', 'CORRELATION_ID', 'TENANT_ID', 'ROUTE')),
    CONSTRAINT deep_trace_sessions_status_ck CHECK (status IN ('ACTIVE', 'EXPIRED', 'CANCELLED', 'CAPPED')),
    CONSTRAINT deep_trace_sessions_max_spans_ck CHECK (max_spans >= 100 AND max_spans <= 5000)
);

CREATE TABLE IF NOT EXISTS trace_sampling_rules (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'OBS_ADMIN',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    rule_name VARCHAR(128) NOT NULL,
    match_criteria JSONB NOT NULL DEFAULT '{}'::jsonb,
    sample_rate DOUBLE PRECISION NOT NULL DEFAULT 0.05,
    is_vip BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_trace_sampling_rules PRIMARY KEY (tenant_id, id),
    CONSTRAINT trace_sampling_rules_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT trace_sampling_rules_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT trace_sampling_rules_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT trace_sampling_rules_name_uk UNIQUE (tenant_id, rule_name),
    CONSTRAINT trace_sampling_rules_public_id_ck CHECK (public_id ~ '^tsr_[0-9a-zA-Z]{26}$'),
    CONSTRAINT trace_sampling_rules_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT trace_sampling_rules_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT trace_sampling_rules_rate_ck CHECK (sample_rate >= 0.0 AND sample_rate <= 1.0)
);

CREATE TABLE IF NOT EXISTS trace_spans_buffer (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'OTEL_TRACER',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    trace_id VARCHAR(32) NOT NULL,
    span_id VARCHAR(16) NOT NULL,
    parent_span_id VARCHAR(16) NOT NULL DEFAULT '',
    span_name VARCHAR(128) NOT NULL,
    span_kind VARCHAR(32) NOT NULL DEFAULT 'INTERNAL',
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    duration_ns BIGINT NOT NULL,
    status_code VARCHAR(32) NOT NULL DEFAULT 'UNSET',
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    events JSONB NOT NULL DEFAULT '[]'::jsonb,
    links JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_trace_spans_buffer PRIMARY KEY (tenant_id, id),
    CONSTRAINT trace_spans_buffer_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT trace_spans_buffer_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT trace_spans_buffer_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT trace_spans_buffer_trace_span_uk UNIQUE (tenant_id, trace_id, span_id),
    CONSTRAINT trace_spans_buffer_public_id_ck CHECK (public_id ~ '^spn_[0-9a-zA-Z]{26}$'),
    CONSTRAINT trace_spans_buffer_trace_id_ck CHECK (length(trace_id) = 32),
    CONSTRAINT trace_spans_buffer_span_id_ck CHECK (length(span_id) = 16),
    CONSTRAINT trace_spans_buffer_status_ck CHECK (status_code IN ('UNSET', 'OK', 'ERROR'))
);

CREATE TABLE IF NOT EXISTS trace_links_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ASYNC_RELAY',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    traceparent VARCHAR(128) NOT NULL,
    tracestate TEXT NOT NULL DEFAULT '',
    link_type VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_trace_links_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT trace_links_registry_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT trace_links_registry_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT trace_links_registry_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT trace_links_registry_saga_step_uk UNIQUE (tenant_id, saga_id, step_name, link_type),
    CONSTRAINT trace_links_registry_public_id_ck CHECK (public_id ~ '^tlr_[0-9a-zA-Z]{26}$'),
    CONSTRAINT trace_links_registry_link_type_ck CHECK (link_type IN ('DISPATCH', 'REPLY', 'TIMER', 'COMPENSATION', 'INTERVENTION'))
);

CREATE INDEX IF NOT EXISTS idx_deep_trace_sessions_lookup
    ON deep_trace_sessions (tenant_id, target_type, target_value, status, expires_at);

CREATE INDEX IF NOT EXISTS idx_trace_sampling_rules_lookup
    ON trace_sampling_rules (tenant_id, enabled, is_vip);

CREATE INDEX IF NOT EXISTS idx_trace_spans_buffer_trace
    ON trace_spans_buffer (tenant_id, trace_id, start_time ASC);

CREATE INDEX IF NOT EXISTS idx_trace_links_registry_saga
    ON trace_links_registry (tenant_id, saga_id, link_type);


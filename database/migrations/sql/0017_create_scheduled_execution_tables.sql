-- migration:      0017
-- description:    create schedules and schedule occurrences tables
-- author:         lead-architect
-- date:           2026-11-10T10:45:00Z
-- depends_on:     0016_create_optimistic_concurrency_control_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         support timezone-aware scheduled executions, misfire handling, and occurrence tracking

CREATE TABLE IF NOT EXISTS schedules (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_SCHEDULER',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    workflow_id VARCHAR(128) NOT NULL,
    schedule_type VARCHAR(32) NOT NULL,
    cron_expression VARCHAR(64) NOT NULL DEFAULT '',
    time_zone VARCHAR(64) NOT NULL,
    dst_policy VARCHAR(32) NOT NULL DEFAULT 'RUN_AT_NEXT_VALID',
    misfire_policy VARCHAR(32) NOT NULL DEFAULT 'RUN_ALL_MISSED',
    overlap_policy VARCHAR(32) NOT NULL DEFAULT 'SKIP_IF_RUNNING',
    max_lateness_seconds INT NOT NULL DEFAULT 3600,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    run_as_principal VARCHAR(128) NOT NULL,
    required_permission VARCHAR(64) NOT NULL DEFAULT 'action:workflow:execute',
    frozen_payload TEXT NOT NULL DEFAULT '{}',
    next_occurrence_at TIMESTAMPTZ NOT NULL,
    last_occurrence_at TIMESTAMPTZ,
    consecutive_failures INT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_schedules PRIMARY KEY (tenant_id, id),
    CONSTRAINT schedules_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT schedules_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT schedules_natural_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT schedules_public_id_ck CHECK (public_id ~ '^sch_[0-9a-zA-Z]{26}$'),
    CONSTRAINT schedules_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT schedules_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT schedules_workflow_id_ck CHECK (workflow_id = btrim(workflow_id) AND workflow_id <> ''),
    CONSTRAINT schedules_timezone_ck CHECK (time_zone = btrim(time_zone) AND time_zone <> ''),
    CONSTRAINT schedules_type_ck CHECK (schedule_type IN ('ONE_OFF', 'RECURRING_CRON')),
    CONSTRAINT schedules_dst_policy_ck CHECK (dst_policy IN ('RUN_AT_NEXT_VALID', 'SKIP', 'RUN_FIRST', 'RUN_SECOND')),
    CONSTRAINT schedules_misfire_policy_ck CHECK (misfire_policy IN ('RUN_ALL_MISSED', 'RUN_ONCE', 'SKIP_MISSED')),
    CONSTRAINT schedules_overlap_policy_ck CHECK (overlap_policy IN ('ALLOW', 'SKIP_IF_RUNNING', 'QUEUE_ONE', 'CANCEL_PREVIOUS')),
    CONSTRAINT schedules_status_ck CHECK (status IN ('ACTIVE', 'PAUSED', 'EXPIRED', 'COMPLETED'))
);

CREATE INDEX IF NOT EXISTS idx_schedules_due
    ON schedules (tenant_id, status, next_occurrence_at);

CREATE TABLE IF NOT EXISTS schedule_occurrences (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_SCHEDULER',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    schedule_id UUID NOT NULL,
    logical_occurrence_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'TRIGGERED',
    dispatched_saga_id UUID,
    failure_reason TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_schedule_occurrences PRIMARY KEY (tenant_id, id),
    CONSTRAINT schedule_occurrences_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT schedule_occurrences_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT schedule_occurrences_logical_uk UNIQUE (tenant_id, schedule_id, logical_occurrence_at),
    CONSTRAINT schedule_occurrences_natural_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT schedule_occurrences_public_id_ck CHECK (public_id ~ '^occ_[0-9a-zA-Z]{26}$'),
    CONSTRAINT schedule_occurrences_status_ck CHECK (status IN ('TRIGGERED', 'DISPATCHED', 'SKIPPED_MISFIRE', 'SKIPPED_OVERLAP', 'EXPIRED', 'DENIED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_schedule_occurrences_schedule
    ON schedule_occurrences (tenant_id, schedule_id);

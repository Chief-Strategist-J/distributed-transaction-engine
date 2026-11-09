-- migration:      0005
-- description:    create sequential execution step journals, context entries and timers
-- author:         lead-architect
-- date:           2026-11-09T11:58:00Z
-- depends_on:     0004_create_crash_recovery_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         support sequential saga execution invariants, durable timers, and step context

ALTER TABLE saga_instances ADD COLUMN IF NOT EXISTS current_step_index INT NOT NULL DEFAULT 0;
ALTER TABLE saga_instances ADD COLUMN IF NOT EXISTS total_steps INT NOT NULL DEFAULT 0;
ALTER TABLE saga_instances ADD COLUMN IF NOT EXISTS execution_phase VARCHAR(32) NOT NULL DEFAULT 'FORWARD';
ALTER TABLE saga_instances ADD COLUMN IF NOT EXISTS pivot_reached BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE saga_instances ADD COLUMN IF NOT EXISTS owner_epoch BIGINT NOT NULL DEFAULT 1;

ALTER TABLE saga_instances DROP CONSTRAINT IF EXISTS saga_instances_execution_phase_ck;
ALTER TABLE saga_instances ADD CONSTRAINT saga_instances_execution_phase_ck CHECK (
    execution_phase IN ('FORWARD', 'COMPENSATING', 'ROLL_FORWARD')
);

ALTER TABLE saga_instances DROP CONSTRAINT IF EXISTS saga_instances_public_id_ck;
ALTER TABLE saga_instances ADD CONSTRAINT saga_instances_public_id_ck CHECK (
    public_id ~ '^(sga|seq)_[0-9a-zA-Z]{26}$'
);

CREATE TABLE IF NOT EXISTS saga_context_entries (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    producer_step_index INT NOT NULL,
    entry_name VARCHAR(128) NOT NULL,
    entry_value JSONB NOT NULL DEFAULT '{}'::jsonb,
    schema_version INT NOT NULL DEFAULT 1,
    correlation_id UUID NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_saga_context_entries PRIMARY KEY (tenant_id, id),
    CONSTRAINT saga_context_entries_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT saga_context_entries_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT saga_context_entries_saga_step_name_uk UNIQUE (tenant_id, saga_id, producer_step_index, entry_name),
    CONSTRAINT saga_context_entries_public_id_ck CHECK (public_id ~ '^ctx_[0-9a-zA-Z]{26}$'),
    CONSTRAINT saga_context_entries_entry_name_ck CHECK (entry_name = btrim(entry_name) AND entry_name <> ''),
    CONSTRAINT saga_context_entries_step_index_ck CHECK (producer_step_index >= 0),
    CONSTRAINT fk_saga_context_entries_saga FOREIGN KEY (tenant_id, saga_id)
        REFERENCES saga_instances (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS saga_timers (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    step_index INT NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    timer_kind VARCHAR(32) NOT NULL,
    timer_state VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    due_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    external_id VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_saga_timers PRIMARY KEY (tenant_id, id),
    CONSTRAINT saga_timers_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT saga_timers_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT saga_timers_unique_trigger UNIQUE (tenant_id, saga_id, step_index, attempt, timer_kind),
    CONSTRAINT saga_timers_public_id_ck CHECK (public_id ~ '^tmr_[0-9a-zA-Z]{26}$'),
    CONSTRAINT saga_timers_kind_ck CHECK (timer_kind IN ('STEP_TIMEOUT', 'RETRY_BACKOFF', 'SAGA_DEADLINE')),
    CONSTRAINT saga_timers_state_ck CHECK (timer_state IN ('PENDING', 'FIRED', 'CANCELLED', 'EXPIRED')),
    CONSTRAINT fk_saga_timers_saga FOREIGN KEY (tenant_id, saga_id)
        REFERENCES saga_instances (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_saga_timers_due ON saga_timers (tenant_id, timer_state, due_at);
CREATE INDEX IF NOT EXISTS idx_saga_context_saga ON saga_context_entries (tenant_id, saga_id);

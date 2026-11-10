-- migration:      0015
-- description:    add owner_epoch to saga instances and create occ contention and pending input tables
-- author:         lead-architect
-- date:           2026-11-10T10:33:00Z
-- depends_on:     0014_create_dead_letter_queue_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide ownership-guarded cas, zero-row multi-cause diagnosis, and durable pending input parking

ALTER TABLE saga_instance_states ADD COLUMN IF NOT EXISTS owner_epoch BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS occ_pending_inputs (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_OCC',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    input_type VARCHAR(32) NOT NULL CHECK (input_type IN ('REPLY', 'TIMER', 'OPERATOR_COMMAND')),
    input_payload JSONB NOT NULL,
    attempt_count INT NOT NULL DEFAULT 1 CHECK (attempt_count >= 1),
    last_conflict_reason TEXT NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'REPROCESSED', 'QUARANTINED')),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_occ_pending_inputs PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_occ_pending_inputs_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uk_occ_pending_inputs_ext UNIQUE (tenant_id, source, external_id)
);

CREATE TABLE IF NOT EXISTS occ_contention_journal (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    attempted_version BIGINT NOT NULL,
    current_version BIGINT NOT NULL,
    attempted_epoch BIGINT NOT NULL,
    current_epoch BIGINT NOT NULL,
    diagnosis VARCHAR(32) NOT NULL CHECK (diagnosis IN ('VERSION_CONFLICT', 'NOT_OWNER', 'STATE_CONFLICT', 'DATA_ERROR', 'SECURITY_INTEGRITY_VIOLATION')),
    worker_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_occ_contention_journal PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS idx_occ_pending_status ON occ_pending_inputs (tenant_id, status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_occ_contention_saga ON occ_contention_journal (tenant_id, saga_id, created_at DESC);

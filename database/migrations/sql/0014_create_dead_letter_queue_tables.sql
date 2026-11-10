-- migration:      0014
-- description:    create dead letter queue records, transitions, and systemic pause tables
-- author:         lead-architect
-- date:           2026-11-10T10:15:00Z
-- depends_on:     0013_create_reconciliation_engine_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide dead-letter quarantine, evidence-based triage, corrective actions, and systemic incident mitigation

ALTER TABLE saga_instance_states DROP CONSTRAINT IF EXISTS saga_instance_states_current_state_ck;
ALTER TABLE saga_instance_states ADD CONSTRAINT saga_instance_states_current_state_ck CHECK (
    current_state IN (
        'CREATED',
        'RUNNING',
        'WAITING',
        'THROTTLED_WAIT',
        'RECONCILING',
        'COMPENSATING',
        'STUCK',
        'COMPLETED',
        'CANCELLED',
        'COMPENSATED',
        'FAILED',
        'NEEDS_ATTENTION'
    )
);

INSERT INTO saga_state_transitions (from_state, to_state) VALUES
    ('RUNNING', 'NEEDS_ATTENTION'),
    ('COMPENSATING', 'NEEDS_ATTENTION'),
    ('WAITING', 'NEEDS_ATTENTION'),
    ('THROTTLED_WAIT', 'NEEDS_ATTENTION'),
    ('NEEDS_ATTENTION', 'RUNNING'),
    ('NEEDS_ATTENTION', 'COMPENSATING'),
    ('NEEDS_ATTENTION', 'FAILED')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dlq_record_transitions (
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dlq_record_transitions PRIMARY KEY (from_state, to_state)
);

INSERT INTO dlq_record_transitions (from_state, to_state) VALUES
    ('QUARANTINED', 'TRIAGED'),
    ('QUARANTINED', 'ESCALATED'),
    ('QUARANTINED', 'ACTION_IN_PROGRESS'),
    ('TRIAGED', 'ACTION_IN_PROGRESS'),
    ('TRIAGED', 'ESCALATED'),
    ('ACTION_IN_PROGRESS', 'RESOLVED'),
    ('ACTION_IN_PROGRESS', 'TRIAGED'),
    ('ACTION_IN_PROGRESS', 'ESCALATED'),
    ('ESCALATED', 'ACTION_IN_PROGRESS'),
    ('ESCALATED', 'TRIAGED')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dead_letter_records (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_SAGA',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    step_index INT NOT NULL CHECK (step_index >= 0),
    step_type VARCHAR(64) NOT NULL,
    attempt_id UUID NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    outcome_class VARCHAR(32) NOT NULL CHECK (outcome_class IN ('NOT_EXECUTED', 'UNKNOWN', 'FAILED')),
    failure_class VARCHAR(32) NOT NULL CHECK (failure_class IN ('POISON', 'DEFINITION_DEFECT', 'RETRY_EXHAUSTED', 'BUSINESS_REJECTION', 'COMPENSATION_FAILURE', 'INTEGRITY_VIOLATION')),
    error_fingerprint VARCHAR(128) NOT NULL CHECK (error_fingerprint = btrim(error_fingerprint) AND error_fingerprint <> ''),
    sanitized_message TEXT NOT NULL DEFAULT '',
    payload_ref TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    money_at_stake_cents BIGINT NOT NULL DEFAULT 0 CHECK (money_at_stake_cents >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD' CHECK (currency ~ '^[A-Z]{3}$'),
    business_deadline TIMESTAMPTZ,
    status VARCHAR(32) NOT NULL DEFAULT 'QUARANTINED' CHECK (status IN ('QUARANTINED', 'TRIAGED', 'ACTION_IN_PROGRESS', 'RESOLVED', 'ESCALATED')),
    disposition VARCHAR(32) CHECK (disposition IS NULL OR disposition IN ('RETRIED_SUCCEEDED', 'RESOLVED_AS_EXECUTED', 'RESOLVED_AS_NOT_EXECUTED', 'CORRECTED', 'SAGA_COMPENSATED', 'SAGA_ROLLED_FORWARD', 'SAGA_COMPLETED_MANUALLY', 'WRITTEN_OFF')),
    actor_id VARCHAR(128),
    second_approver_id VARCHAR(128),
    disposition_reason TEXT,
    evidence_ref TEXT,
    corrective_attempt_id UUID,
    corrective_idempotency_key VARCHAR(128),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dead_letter_records PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_dead_letter_records_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uk_dead_letter_records_attempt UNIQUE (tenant_id, saga_id, step_index, attempt_id),
    CONSTRAINT uk_dead_letter_records_ext UNIQUE (tenant_id, source, external_id),
    CONSTRAINT dead_letter_records_resolved_ck CHECK ((status = 'RESOLVED') = (disposition IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS dlq_systemic_incidents (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_DLQ',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    fingerprint VARCHAR(128) NOT NULL,
    step_type VARCHAR(64) NOT NULL,
    incident_status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE' CHECK (incident_status IN ('ACTIVE', 'RESOLVED')),
    affected_count INT NOT NULL DEFAULT 1 CHECK (affected_count >= 1),
    paused_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    unpaused_at TIMESTAMPTZ,
    owner_team VARCHAR(64) NOT NULL DEFAULT 'platform-team',
    review_deadline TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dlq_systemic_incidents PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_dlq_systemic_incidents_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uk_dlq_systemic_incidents_ext UNIQUE (tenant_id, source, external_id)
);

CREATE TABLE IF NOT EXISTS dlq_step_type_pauses (
    tenant_id VARCHAR(64) NOT NULL,
    step_type VARCHAR(64) NOT NULL,
    incident_id UUID NOT NULL,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    review_deadline TIMESTAMPTZ NOT NULL,
    owner_team VARCHAR(64) NOT NULL,
    CONSTRAINT pk_dlq_step_type_pauses PRIMARY KEY (tenant_id, step_type)
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_records_triage ON dead_letter_records (tenant_id, status, business_deadline ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_dead_letter_records_fingerprint ON dead_letter_records (tenant_id, error_fingerprint, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dead_letter_records_saga ON dead_letter_records (tenant_id, saga_id);
CREATE INDEX IF NOT EXISTS idx_dlq_systemic_active ON dlq_systemic_incidents (tenant_id, fingerprint) WHERE incident_status = 'ACTIVE';

-- migration:      0001
-- description:    create saga state machine and transition audit tables
-- author:         chief-strategist
-- date:           2026-11-02T16:50:00Z
-- depends_on:     NONE
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         initial schema creation for transaction-state-machine feature

CREATE TABLE IF NOT EXISTS saga_state_transitions (
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_saga_state_transitions PRIMARY KEY (from_state, to_state)
);

INSERT INTO saga_state_transitions (from_state, to_state) VALUES
    ('CREATED', 'RUNNING'),
    ('CREATED', 'CANCELLED'),
    ('RUNNING', 'RUNNING'),
    ('RUNNING', 'COMPLETED'),
    ('RUNNING', 'COMPENSATING'),
    ('RUNNING', 'THROTTLED_WAIT'),
    ('RUNNING', 'RECONCILING'),
    ('RUNNING', 'WAITING'),
    ('WAITING', 'RUNNING'),
    ('WAITING', 'COMPENSATING'),
    ('THROTTLED_WAIT', 'RUNNING'),
    ('RECONCILING', 'RUNNING'),
    ('RECONCILING', 'COMPENSATING'),
    ('RECONCILING', 'STUCK'),
    ('COMPENSATING', 'COMPENSATING'),
    ('COMPENSATING', 'COMPENSATED'),
    ('COMPENSATING', 'STUCK'),
    ('STUCK', 'COMPENSATING'),
    ('STUCK', 'FAILED')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS saga_instance_states (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    current_state VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_saga_instance_states PRIMARY KEY (tenant_id, id),
    CONSTRAINT saga_instance_states_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT saga_instance_states_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT saga_instance_states_public_id_ck CHECK (public_id ~ '^sag_[0-9a-zA-Z]{26}$'),
    CONSTRAINT saga_instance_states_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT saga_instance_states_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT saga_instance_states_current_state_ck CHECK (
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
            'FAILED'
        )
    ),
    CONSTRAINT saga_instance_states_version_ck CHECK (version >= 1),
    CONSTRAINT saga_instance_states_fencing_token_ck CHECK (fencing_token >= 0)
);

CREATE TABLE IF NOT EXISTS state_transition_audit (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    version_before BIGINT NOT NULL,
    version_after BIGINT NOT NULL,
    fencing_token BIGINT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_state_transition_audit PRIMARY KEY (tenant_id, id),
    CONSTRAINT state_transition_audit_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT state_transition_audit_public_id_ck CHECK (public_id ~ '^aud_[0-9a-zA-Z]{26}$'),
    CONSTRAINT state_transition_audit_reason_ck CHECK (reason = btrim(reason) AND reason <> ''),
    CONSTRAINT fk_state_transition_audit_saga FOREIGN KEY (tenant_id, saga_id)
        REFERENCES saga_instance_states (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_saga_instance_states_lookup 
    ON saga_instance_states (tenant_id, id, current_state);

CREATE INDEX IF NOT EXISTS idx_saga_instance_states_corr 
    ON saga_instance_states (tenant_id, correlation_id);

CREATE INDEX IF NOT EXISTS idx_state_transition_audit_saga 
    ON state_transition_audit (tenant_id, saga_id, created_at);

-- migration:      0019
-- description:    create tamper-evident audit outbox, immutable ledger, quarantine, and checkpoint tables
-- author:         lead-architect
-- date:           2026-11-10T11:25:00Z
-- depends_on:     0018_create_workflow_versioning_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide tamper-evident hash-chained audit logging, outbox decoupling, quarantine, and checkpoint anchoring per tenant

CREATE TABLE IF NOT EXISTS audit_outbox (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    audit_id VARCHAR(128) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    action VARCHAR(128) NOT NULL,
    actor_id VARCHAR(128) NOT NULL,
    actor_ip VARCHAR(64) NOT NULL DEFAULT '',
    on_behalf_of VARCHAR(128) NOT NULL DEFAULT '',
    target_resource VARCHAR(256) NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    chained_at TIMESTAMPTZ,
    CONSTRAINT pk_audit_outbox PRIMARY KEY (tenant_id, id),
    CONSTRAINT audit_outbox_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT audit_outbox_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT audit_outbox_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT audit_outbox_audit_id_uk UNIQUE (tenant_id, audit_id),
    CONSTRAINT audit_outbox_public_id_ck CHECK (public_id ~ '^aob_[0-9a-zA-Z]{26}$'),
    CONSTRAINT audit_outbox_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT audit_outbox_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT audit_outbox_audit_id_ck CHECK (audit_id = btrim(audit_id) AND audit_id <> ''),
    CONSTRAINT audit_outbox_action_ck CHECK (action = btrim(action) AND action <> ''),
    CONSTRAINT audit_outbox_actor_id_ck CHECK (actor_id = btrim(actor_id) AND actor_id <> ''),
    CONSTRAINT audit_outbox_status_ck CHECK (status IN ('PENDING', 'CHAINED', 'FAILED')),
    CONSTRAINT audit_outbox_payload_ck CHECK (jsonb_typeof(payload) = 'object')
);

CREATE TABLE IF NOT EXISTS immutable_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    audit_id VARCHAR(128) NOT NULL,
    segment_id UUID NOT NULL,
    sequence_id BIGINT NOT NULL,
    key_id VARCHAR(64) NOT NULL,
    actor_id VARCHAR(128) NOT NULL,
    actor_ip VARCHAR(64) NOT NULL DEFAULT '',
    action VARCHAR(128) NOT NULL,
    prev_hash CHAR(64) NOT NULL,
    curr_hash CHAR(64) NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_immutable_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT immutable_audit_log_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT immutable_audit_log_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT immutable_audit_log_seq_uk UNIQUE (tenant_id, segment_id, sequence_id),
    CONSTRAINT immutable_audit_log_hash_uk UNIQUE (tenant_id, curr_hash),
    CONSTRAINT immutable_audit_log_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT immutable_audit_log_audit_id_uk UNIQUE (tenant_id, audit_id),
    CONSTRAINT immutable_audit_log_public_id_ck CHECK (public_id ~ '^aud_[0-9a-zA-Z]{26}$'),
    CONSTRAINT immutable_audit_log_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT immutable_audit_log_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT immutable_audit_log_audit_id_ck CHECK (audit_id = btrim(audit_id) AND audit_id <> ''),
    CONSTRAINT immutable_audit_log_key_id_ck CHECK (key_id = btrim(key_id) AND key_id <> ''),
    CONSTRAINT immutable_audit_log_actor_id_ck CHECK (actor_id = btrim(actor_id) AND actor_id <> ''),
    CONSTRAINT immutable_audit_log_action_ck CHECK (action = btrim(action) AND action <> ''),
    CONSTRAINT immutable_audit_log_prev_hash_ck CHECK (length(prev_hash) = 64),
    CONSTRAINT immutable_audit_log_curr_hash_ck CHECK (length(curr_hash) = 64),
    CONSTRAINT immutable_audit_log_raw_payload_ck CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT immutable_audit_log_seq_ck CHECK (sequence_id >= 1)
);

CREATE TABLE IF NOT EXISTS audit_quarantine (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    audit_id VARCHAR(128) NOT NULL,
    quarantine_reason VARCHAR(64) NOT NULL,
    raw_record JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_details TEXT NOT NULL DEFAULT '',
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_audit_quarantine PRIMARY KEY (tenant_id, id),
    CONSTRAINT audit_quarantine_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT audit_quarantine_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT audit_quarantine_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT audit_quarantine_public_id_ck CHECK (public_id ~ '^quar_[0-9a-zA-Z]{26}$'),
    CONSTRAINT audit_quarantine_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT audit_quarantine_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT audit_quarantine_reason_ck CHECK (quarantine_reason IN ('ATTRIBUTION_MISSING', 'SCHEMA_INVALID', 'PAYLOAD_CORRUPT')),
    CONSTRAINT audit_quarantine_raw_record_ck CHECK (jsonb_typeof(raw_record) = 'object')
);

CREATE TABLE IF NOT EXISTS audit_checkpoints (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'AUDIT_CHECKPOINT_WORKER',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    segment_id UUID NOT NULL,
    last_sequence_id BIGINT NOT NULL,
    head_hash CHAR(64) NOT NULL,
    signature TEXT NOT NULL,
    key_id VARCHAR(64) NOT NULL,
    event_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_audit_checkpoints PRIMARY KEY (tenant_id, id),
    CONSTRAINT audit_checkpoints_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT audit_checkpoints_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT audit_checkpoints_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT audit_checkpoints_public_id_ck CHECK (public_id ~ '^ack_[0-9a-zA-Z]{26}$'),
    CONSTRAINT audit_checkpoints_head_hash_ck CHECK (length(head_hash) = 64),
    CONSTRAINT audit_checkpoints_signature_ck CHECK (signature = btrim(signature) AND signature <> ''),
    CONSTRAINT audit_checkpoints_key_id_ck CHECK (key_id = btrim(key_id) AND key_id <> ''),
    CONSTRAINT audit_checkpoints_seq_ck CHECK (last_sequence_id >= 1),
    CONSTRAINT audit_checkpoints_count_ck CHECK (event_count >= 1)
);

CREATE TABLE IF NOT EXISTS audit_chain_verifications (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'AUDIT_VERIFIER',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    segment_id UUID NOT NULL,
    verified_up_to_sequence BIGINT NOT NULL,
    total_events_checked INT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'VALID',
    failure_sequence_id BIGINT,
    failure_reason TEXT NOT NULL DEFAULT '',
    verified_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_audit_chain_verifications PRIMARY KEY (tenant_id, id),
    CONSTRAINT audit_chain_verifications_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT audit_chain_verifications_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT audit_chain_verifications_external_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT audit_chain_verifications_public_id_ck CHECK (public_id ~ '^acv_[0-9a-zA-Z]{26}$'),
    CONSTRAINT audit_chain_verifications_status_ck CHECK (status IN ('VALID', 'COMPROMISED')),
    CONSTRAINT audit_chain_verifications_verified_by_ck CHECK (verified_by = btrim(verified_by) AND verified_by <> ''),
    CONSTRAINT audit_chain_verifications_checked_ck CHECK (total_events_checked >= 0)
);

CREATE INDEX IF NOT EXISTS idx_audit_outbox_pending
    ON audit_outbox (tenant_id, status, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_immutable_audit_log_seq
    ON immutable_audit_log (tenant_id, segment_id, sequence_id ASC);

CREATE INDEX IF NOT EXISTS idx_immutable_audit_log_action
    ON immutable_audit_log (tenant_id, action, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_quarantine_lookup
    ON audit_quarantine (tenant_id, resolved, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_checkpoints_lookup
    ON audit_checkpoints (tenant_id, segment_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_chain_verifications_lookup
    ON audit_chain_verifications (tenant_id, segment_id, created_at DESC);

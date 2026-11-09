-- Migration: 0008_create_transactional_outbox_tables.sql
-- Direction: Up

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    public_id VARCHAR(30) NOT NULL UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_sequence BIGINT NOT NULL,
    destination_topic VARCHAR(128) NOT NULL,
    partition_key VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    lease_owner VARCHAR(128),
    lease_expires_at TIMESTAMPTZ,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,

    CONSTRAINT outbox_events_public_id_ck CHECK (public_id ~ '^oxe_[0-9a-zA-Z]{26}$'),
    CONSTRAINT outbox_events_status_ck CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED', 'PARKED')),
    CONSTRAINT outbox_events_payload_json_ck CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT outbox_events_headers_json_ck CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT outbox_events_unique_seq UNIQUE (tenant_id, aggregate_id, aggregate_sequence)
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending_poll
    ON outbox_events(tenant_id, destination_topic, aggregate_sequence ASC)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_outbox_lease_expiry
    ON outbox_events(lease_expires_at)
    WHERE status = 'IN_FLIGHT';

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox_events(tenant_id, aggregate_id, aggregate_sequence);

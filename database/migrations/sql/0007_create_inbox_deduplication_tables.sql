-- migration:      0007
-- description:    create inbox deduplication journal table
-- author:         lead-architect
-- date:           2026-11-09T13:14:00Z
-- depends_on:     0006_create_parallel_execution_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         support exactly-once message processing, idempotency verification, and integrity checks

CREATE TABLE IF NOT EXISTS inbox_messages (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    handler_name VARCHAR(128) NOT NULL,
    handler_generation INT NOT NULL DEFAULT 1,
    content_hash CHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'APPLIED',
    attempt_count INT NOT NULL DEFAULT 1,
    producer_event_time TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_inbox_messages PRIMARY KEY (tenant_id, id),
    CONSTRAINT inbox_messages_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT inbox_messages_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT inbox_messages_dedup_uk UNIQUE (tenant_id, handler_name, handler_generation, external_id),
    CONSTRAINT inbox_messages_public_id_ck CHECK (public_id ~ '^inb_[0-9a-zA-Z]{26}$'),
    CONSTRAINT inbox_messages_handler_name_ck CHECK (handler_name = btrim(handler_name) AND handler_name <> ''),
    CONSTRAINT inbox_messages_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT inbox_messages_hash_ck CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT inbox_messages_status_ck CHECK (status IN ('APPLIED', 'HELD', 'QUARANTINED')),
    CONSTRAINT inbox_messages_attempt_ck CHECK (attempt_count >= 1),
    CONSTRAINT inbox_messages_generation_ck CHECK (handler_generation >= 1)
);

CREATE INDEX IF NOT EXISTS idx_inbox_messages_prune 
    ON inbox_messages (tenant_id, producer_event_time);


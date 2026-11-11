-- migration:      0027
-- description:    add next_attempt_at so failed outbox publishes back off, and index the relay readiness scan
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0008_create_transactional_outbox_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- lock_impact:    ADD COLUMN without default is metadata only; CREATE INDEX takes SHARE lock on outbox_events (use CONCURRENTLY outside the migrator for very large tables)
-- nullable:       next_attempt_at is NULL when the event is immediately eligible for relay (never failed), which is a real business state

ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_outbox_relay_ready
    ON outbox_events (tenant_id, destination_topic, aggregate_id, aggregate_sequence)
    WHERE status IN ('PENDING', 'IN_FLIGHT', 'PARKED');

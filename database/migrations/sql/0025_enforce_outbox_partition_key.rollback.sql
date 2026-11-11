-- migration:      0025
-- description:    rollback saga_id partition key enforcement on outbox_events
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0025_enforce_outbox_partition_key.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_events_partition_key_ck;

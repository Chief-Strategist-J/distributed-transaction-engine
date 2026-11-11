-- migration:      0027
-- description:    rollback outbox relay backoff column and readiness index
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0027_outbox_relay_backoff.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

DROP INDEX IF EXISTS idx_outbox_relay_ready;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS next_attempt_at;

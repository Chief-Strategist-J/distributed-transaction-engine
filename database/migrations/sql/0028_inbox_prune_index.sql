-- migration:      0028
-- description:    partial index so bounded retention pruning of applied inbox records is an index range scan
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0007_create_inbox_deduplication_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- lock_impact:    CREATE INDEX takes a SHARE lock on inbox_messages (use CONCURRENTLY outside the migrator for very large tables)

CREATE INDEX IF NOT EXISTS idx_inbox_messages_expiry
    ON inbox_messages (created_at)
    WHERE status = 'APPLIED';

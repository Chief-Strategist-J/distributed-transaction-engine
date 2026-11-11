-- migration:      0028
-- description:    rollback inbox pruning index
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0028_inbox_prune_index.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

DROP INDEX IF EXISTS idx_inbox_messages_expiry;

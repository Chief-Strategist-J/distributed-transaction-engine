-- migration:      0006_rollback
-- description:    rollback parallel execution group and branch tables
-- author:         lead-architect
-- date:           2026-11-09T13:11:00Z
-- depends_on:     0006_create_parallel_execution_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

DROP TABLE IF EXISTS parallel_branches;
DROP TABLE IF EXISTS parallel_groups;

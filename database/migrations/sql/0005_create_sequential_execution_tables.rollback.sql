-- migration:      0005_rollback
-- description:    rollback sequential execution tables and instance columns
-- author:         lead-architect
-- date:           2026-11-09T11:58:00Z
-- depends_on:     0005_create_sequential_execution_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

DROP TABLE IF EXISTS saga_timers;
DROP TABLE IF EXISTS saga_context_entries;

ALTER TABLE saga_instances DROP CONSTRAINT IF EXISTS saga_instances_execution_phase_ck;
ALTER TABLE saga_instances DROP COLUMN IF EXISTS owner_epoch;
ALTER TABLE saga_instances DROP COLUMN IF EXISTS pivot_reached;
ALTER TABLE saga_instances DROP COLUMN IF EXISTS execution_phase;
ALTER TABLE saga_instances DROP COLUMN IF EXISTS total_steps;
ALTER TABLE saga_instances DROP COLUMN IF EXISTS current_step_index;

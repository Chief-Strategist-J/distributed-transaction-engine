-- migration:      0002_rollback
-- description:    rollback workflow definitions and step topology tables
-- author:         lead-architect
-- date:           2026-11-03T10:00:00Z
-- depends_on:     0002_create_workflow_definitions_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

DROP INDEX IF EXISTS idx_workflow_definition_steps_wf;
DROP INDEX IF EXISTS idx_workflow_definitions_public;
DROP INDEX IF EXISTS idx_workflow_definitions_lookup;

DROP TABLE IF EXISTS workflow_definition_steps CASCADE;
DROP TABLE IF EXISTS workflow_definitions CASCADE;


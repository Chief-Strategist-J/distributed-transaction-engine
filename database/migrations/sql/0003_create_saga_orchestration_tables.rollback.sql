-- rollback:    0003
-- description: drop saga instances and step execution journals
-- author:      lead-architect
-- date:        2026-11-03T11:40:00Z
-- reversible:  YES

DROP TABLE IF EXISTS saga_step_executions CASCADE;
DROP TABLE IF EXISTS saga_instances CASCADE;


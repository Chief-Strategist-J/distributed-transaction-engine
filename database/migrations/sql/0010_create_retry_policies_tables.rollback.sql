-- rollback: 0010_create_retry_policies_tables.sql
DROP INDEX IF EXISTS idx_step_retries_next_attempt;
DROP INDEX IF EXISTS idx_step_retries_saga_step;
DROP TABLE IF EXISTS step_retries CASCADE;
DROP TABLE IF EXISTS retry_policies CASCADE;


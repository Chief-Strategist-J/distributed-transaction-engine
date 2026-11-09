-- rollback: 0009_create_idempotency_keys_tables.sql
DROP INDEX IF EXISTS idx_idempotency_keys_leases;
DROP INDEX IF EXISTS idx_idempotency_keys_expires_at;
DROP TABLE IF EXISTS idempotency_keys CASCADE;


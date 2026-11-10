-- rollback migration: 0019
-- description:        drop audit trail tables

DROP TABLE IF EXISTS audit_chain_verifications CASCADE;
DROP TABLE IF EXISTS audit_checkpoints CASCADE;
DROP TABLE IF EXISTS audit_quarantine CASCADE;
DROP TABLE IF EXISTS immutable_audit_log CASCADE;
DROP TABLE IF EXISTS audit_outbox CASCADE;

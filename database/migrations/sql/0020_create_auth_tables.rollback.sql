-- rollback migration: 0020
-- description:        drop authentication and authorization tables

DROP TABLE IF EXISTS provider_replay_cache CASCADE;
DROP TABLE IF EXISTS revocation_records CASCADE;
DROP TABLE IF EXISTS auth_decision_audit CASCADE;
DROP TABLE IF EXISTS service_identities CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;


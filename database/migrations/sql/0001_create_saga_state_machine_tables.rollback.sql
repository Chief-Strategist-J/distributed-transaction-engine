-- migration:      0001
-- description:    rollback saga state machine and transition audit tables
-- author:         chief-strategist
-- date:           2026-11-02T16:50:00Z
-- depends_on:     NONE
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback initial schema creation for transaction-state-machine feature

DROP INDEX IF EXISTS idx_state_transition_audit_saga;
DROP INDEX IF EXISTS idx_saga_instance_states_corr;
DROP INDEX IF EXISTS idx_saga_instance_states_lookup;
DROP TABLE IF EXISTS state_transition_audit;
DROP TABLE IF EXISTS saga_instance_states;
DROP TABLE IF EXISTS saga_state_transitions;

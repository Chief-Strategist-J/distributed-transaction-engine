DROP TABLE IF EXISTS occ_contention_journal CASCADE;
DROP TABLE IF EXISTS occ_pending_inputs CASCADE;
ALTER TABLE saga_instance_states DROP COLUMN IF EXISTS owner_epoch;

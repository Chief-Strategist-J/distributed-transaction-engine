-- migration:      0025
-- description:    enforce saga_id as the mandatory non-empty Kafka partition key for every outbox event
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0008_create_transactional_outbox_tables.sql
-- reversible:     YES
-- lock_risk:      MEDIUM
-- rows_affected:  every outbox_events row whose partition_key differs from saga_id
-- lock_impact:    UPDATE takes row locks only; ADD CONSTRAINT NOT VALID takes a brief ACCESS EXCLUSIVE lock;
--                 VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE and does not block reads or writes
-- data_check:     SELECT COUNT(*) FROM outbox_events WHERE btrim(partition_key) = '' OR partition_key <> saga_id::text

UPDATE outbox_events
SET partition_key = saga_id::text
WHERE partition_key <> saga_id::text;

DO $$
DECLARE
    violations BIGINT;
BEGIN
    SELECT COUNT(*) INTO violations
    FROM outbox_events
    WHERE btrim(partition_key) = '' OR partition_key <> saga_id::text;
    IF violations > 0 THEN
        RAISE EXCEPTION 'outbox partition key backfill left % violating rows', violations;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'outbox_events_partition_key_ck' AND conrelid = 'outbox_events'::regclass) THEN
        ALTER TABLE outbox_events
            ADD CONSTRAINT outbox_events_partition_key_ck
            CHECK (btrim(partition_key) <> '' AND partition_key = saga_id::text) NOT VALID;
    END IF;
END
$$;

ALTER TABLE outbox_events VALIDATE CONSTRAINT outbox_events_partition_key_ck;

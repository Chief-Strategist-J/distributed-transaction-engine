-- migration:      0004
-- description:    rollback crash recovery schema additions
-- author:         lead-architect
-- date:           2026-11-03T15:00:00Z
-- reversible:     YES

DROP INDEX IF EXISTS idx_saga_instances_orphan_scan;

ALTER TABLE saga_instances DROP CONSTRAINT IF EXISTS saga_instances_state_ck;
ALTER TABLE saga_instances ADD CONSTRAINT saga_instances_state_ck CHECK (
    current_state IN (
        'CREATED',
        'RUNNING',
        'ROLLING_FORWARD',
        'STUCK_FORWARD',
        'COMPENSATING',
        'COMPENSATION_STUCK',
        'COMPLETED',
        'FAILED',
        'FAILED_COMPENSATED',
        'CANCELLED'
    )
);

ALTER TABLE saga_instances DROP COLUMN IF EXISTS takeover_count;


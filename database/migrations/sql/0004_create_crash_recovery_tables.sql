-- migration:      0004
-- description:    add takeover_count and recovery partial index for crash recovery feature
-- author:         lead-architect
-- date:           2026-11-03T15:00:00Z
-- depends_on:     0003_create_saga_orchestration_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         support crash recovery lease takeover protocol and poison prevention

ALTER TABLE saga_instances ADD COLUMN IF NOT EXISTS takeover_count INT NOT NULL DEFAULT 0;

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
        'CANCELLED',
        'PAUSED_POISON'
    )
);

CREATE INDEX IF NOT EXISTS idx_saga_instances_orphan_scan 
ON saga_instances (lease_until) 
WHERE current_state IN ('CREATED', 'RUNNING', 'ROLLING_FORWARD', 'COMPENSATING');


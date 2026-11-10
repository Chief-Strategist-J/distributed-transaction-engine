package queries

const (
	FLOW_INSERT_SCHEDULE = `
INSERT INTO schedules (
    id, public_id, tenant_id, source, external_id, correlation_id, workflow_id,
    schedule_type, cron_expression, time_zone, dst_policy, misfire_policy,
    overlap_policy, max_lateness_seconds, status, run_as_principal,
    required_permission, frozen_payload, next_occurrence_at, last_occurrence_at,
    consecutive_failures, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
);
`

	FLOW_SELECT_DUE_SCHEDULES = `
SELECT 
    id, public_id, tenant_id, source, external_id, correlation_id, workflow_id,
    schedule_type, cron_expression, time_zone, dst_policy, misfire_policy,
    overlap_policy, max_lateness_seconds, status, run_as_principal,
    required_permission, frozen_payload, next_occurrence_at, last_occurrence_at,
    consecutive_failures, version, created_at, updated_at
FROM schedules
WHERE tenant_id = $1 AND status = 'ACTIVE' AND next_occurrence_at <= $2
ORDER BY next_occurrence_at ASC
LIMIT $3;
`

	FLOW_ADVANCE_SCHEDULE = `
UPDATE schedules
SET 
    next_occurrence_at = $1,
    last_occurrence_at = $2,
    version = version + 1,
    updated_at = NOW()
WHERE tenant_id = $3 AND id = $4 AND version = $5;
`

	FLOW_INSERT_OCCURRENCE = `
INSERT INTO schedule_occurrences (
    id, public_id, tenant_id, source, external_id, correlation_id, schedule_id,
    logical_occurrence_at, status, dispatched_saga_id, failure_reason,
    version, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
);
`
)

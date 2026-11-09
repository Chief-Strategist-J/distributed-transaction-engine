package queries

const (
	FLOW_CREATE_COMPENSATION_PLAN = `
INSERT INTO saga_compensation_plans (
    id, public_id, tenant_id, saga_id, current_state,
    total_steps, compensated_steps, failed_steps, residuals, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5,
    $6, 0, 0, '[]'::jsonb, 1, NOW(), NOW()
);`

	FLOW_INSERT_STEP_COMPENSATION = `
INSERT INTO saga_step_compensations (
    id, public_id, tenant_id, plan_id, saga_id, step_id, step_name,
    execution_order, status, forward_execution_id, compensation_action,
    compensation_payload, residual_payload, idempotency_key, attempt_count,
    version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11,
    $12::jsonb, $13::jsonb, $14, 0,
    1, NOW(), NOW()
);`

	FLOW_GET_COMPENSATION_PLAN = `
SELECT id, public_id, tenant_id, saga_id, current_state,
       total_steps, compensated_steps, failed_steps, residuals,
       version, created_at, updated_at, completed_at
FROM saga_compensation_plans
WHERE tenant_id = $1 AND (saga_id = $2 OR id = $2);`

	FLOW_GET_STEP_COMPENSATIONS = `
SELECT id, public_id, tenant_id, plan_id, saga_id, step_id, step_name,
       execution_order, status, forward_execution_id, compensation_action,
       compensation_payload, residual_payload, idempotency_key, attempt_count,
       last_error, version, created_at, updated_at, compensated_at
FROM saga_step_compensations
WHERE tenant_id = $1 AND plan_id = $2
ORDER BY execution_order ASC;`

	FLOW_UPDATE_STEP_COMPENSATION_OUTCOME = `
UPDATE saga_step_compensations
SET status = $1::varchar,
    residual_payload = $2::jsonb,
    last_error = $3,
    compensated_at = CASE WHEN $1::varchar = 'COMPENSATED' THEN NOW() ELSE compensated_at END,
    version = version + 1,
    updated_at = NOW()
WHERE tenant_id = $4
  AND plan_id = $5
  AND step_id = $6;`

	FLOW_UPDATE_COMPENSATION_PLAN_CAS = `
UPDATE saga_compensation_plans
SET current_state = $1::varchar,
    compensated_steps = $2,
    failed_steps = $3,
    residuals = $4::jsonb,
    completed_at = CASE WHEN $1::varchar IN ('FULLY_COMPENSATED', 'COMPENSATED_WITH_RESIDUALS') THEN NOW() ELSE completed_at END,
    version = version + 1,
    updated_at = NOW()
WHERE tenant_id = $5
  AND id = $6
  AND version = $7;`
)

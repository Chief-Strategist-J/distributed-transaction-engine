package queries

const (
	FLOW_CREATE_PARALLEL_GROUP = `
INSERT INTO parallel_groups (
    id, public_id, tenant_id, saga_id, join_policy, quorum_count,
    current_state, total_branches, succeeded_branches, failed_branches,
    version, correlation_id, source, external_id, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, 0, 0,
    1, $9, 'SYSTEM', $10, NOW(), NOW()
);`

	FLOW_CREATE_PARALLEL_BRANCH = `
INSERT INTO parallel_branches (
    id, public_id, tenant_id, group_id, saga_id, branch_id, branch_name,
    role, effect_type, current_state, idempotency_key, branch_input,
    branch_output, correlation_id, source, external_id, version,
    created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12,
    $13, $14, 'SYSTEM', $15, 1,
    NOW(), NOW()
);`

	FLOW_GET_PARALLEL_GROUP = `
SELECT id, public_id, tenant_id, saga_id, join_policy, quorum_count,
       current_state, total_branches, succeeded_branches, failed_branches,
       version, correlation_id, created_at, updated_at
FROM parallel_groups
WHERE tenant_id = $1 AND id = $2;`

	FLOW_GET_GROUP_BRANCHES = `
SELECT id, public_id, tenant_id, group_id, saga_id, branch_id, branch_name,
       role, effect_type, current_state, idempotency_key, branch_input,
       branch_output, error_message, version, created_at, updated_at
FROM parallel_branches
WHERE tenant_id = $1 AND group_id = $2;`

	FLOW_UPDATE_BRANCH_OUTCOME = `
UPDATE parallel_branches
SET current_state = $1,
    branch_output = $2,
    error_message = $3,
    resolved_at = NOW(),
    version = version + 1,
    updated_at = NOW()
WHERE tenant_id = $4 AND group_id = $5 AND branch_id = $6;`

	FLOW_UPDATE_GROUP_CAS = `
UPDATE parallel_groups
SET current_state = $1,
    succeeded_branches = $2,
    failed_branches = $3,
    version = version + 1,
    updated_at = NOW()
WHERE tenant_id = $4
  AND id = $5
  AND version = $6;`
)

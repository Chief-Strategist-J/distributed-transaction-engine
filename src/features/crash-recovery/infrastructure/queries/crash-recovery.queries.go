package queries

const (
	FLOW_FIND_EXPIRED_LEASES = `
SELECT id, public_id, tenant_id, current_state, fencing_token, 
       workflow_definition_id, workflow_version, spec_hash, lease_owner, 
       lease_until, takeover_count, initial_payload
FROM saga_instances
WHERE current_state IN ('CREATED', 'RUNNING', 'ROLLING_FORWARD', 'COMPENSATING')
  AND lease_until IS NOT NULL
  AND lease_until < $1
ORDER BY lease_until ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;
`

	FLOW_TAKEOVER_LEASE = `
UPDATE saga_instances
SET lease_owner = $1,
    lease_until = $2,
    fencing_token = fencing_token + 1,
    takeover_count = takeover_count + 1,
    updated_at = $3
WHERE tenant_id = $4
  AND id = $5
  AND fencing_token = $6
  AND (lease_until IS NULL OR lease_until < $3)
RETURNING fencing_token, takeover_count;
`

	FLOW_MARK_POISONED = `
UPDATE saga_instances
SET current_state = 'PAUSED_POISON',
    lease_owner = '',
    lease_until = NULL,
    updated_at = $1
WHERE tenant_id = $2
  AND id = $3
  AND fencing_token = $4;
`

	FLOW_RENEW_HEARTBEAT = `
UPDATE saga_instances
SET lease_until = $1,
    updated_at = $2
WHERE tenant_id = $3
  AND id = $4
  AND lease_owner = $5
  AND fencing_token = $6;
`

	FLOW_LOAD_STEP_EXECUTIONS = `
SELECT id, public_id, tenant_id, saga_id, step_id, execution_state,
       attempt_count, step_input, step_output, error_message, idempotency_key
FROM saga_step_executions
WHERE tenant_id = $1 AND saga_id = $2
ORDER BY created_at ASC;
`

	FLOW_UPDATE_STEP_EXECUTION = `
UPDATE saga_step_executions
SET execution_state = $1,
    attempt_count = $2,
    step_output = $3,
    error_message = $4,
    updated_at = $5
WHERE tenant_id = $6 AND saga_id = $7 AND step_id = $8;
`

	FLOW_RELEASE_LEASE = `
UPDATE saga_instances
SET lease_owner = '',
    lease_until = NULL,
    updated_at = $1
WHERE tenant_id = $2 AND id = $3 AND fencing_token = $4;
`
)

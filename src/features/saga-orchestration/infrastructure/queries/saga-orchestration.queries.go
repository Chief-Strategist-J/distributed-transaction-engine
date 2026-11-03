package queries

const (
	InsertSagaInstance = `
INSERT INTO saga_instances (
    id, public_id, tenant_id, workflow_definition_id, workflow_version,
    spec_hash, source, external_id, input_hash, correlation_id, current_state,
    version, fencing_token, lease_owner, lease_until, deadline_at,
    initial_payload, final_output, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20);`

	InsertSagaStepExecution = `
INSERT INTO saga_step_executions (
    id, public_id, tenant_id, saga_id, step_id,
    execution_state, attempt_count, step_input, step_output,
    error_message, idempotency_key, started_at, completed_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);`

	SelectSagaByID = `
SELECT id, public_id, tenant_id, workflow_definition_id, workflow_version,
       spec_hash, source, external_id, input_hash, correlation_id, current_state,
       version, fencing_token, lease_owner, lease_until, deadline_at,
       initial_payload, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1 AND id = $2;`

	SelectSagaByPublicID = `
SELECT id, public_id, tenant_id, workflow_definition_id, workflow_version,
       spec_hash, source, external_id, input_hash, correlation_id, current_state,
       version, fencing_token, lease_owner, lease_until, deadline_at,
       initial_payload, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1 AND public_id = $2;`

	SelectSagaByExternalID = `
SELECT id, public_id, tenant_id, workflow_definition_id, workflow_version,
       spec_hash, source, external_id, input_hash, correlation_id, current_state,
       version, fencing_token, lease_owner, lease_until, deadline_at,
       initial_payload, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1 AND workflow_definition_id = $2 AND external_id = $3;`

	SelectSagaSteps = `
SELECT id, public_id, tenant_id, saga_id, step_id,
       execution_state, attempt_count, step_input, step_output,
       error_message, idempotency_key, started_at, completed_at, created_at, updated_at
FROM saga_step_executions
WHERE tenant_id = $1 AND saga_id = $2
ORDER BY created_at ASC;`

	UpdateSagaStateCAS = `
UPDATE saga_instances
SET current_state = $1, version = version + 1, updated_at = $2
WHERE tenant_id = $3 AND id = $4 AND version = $5;`

	UpdateSagaStateWithFencing = `
UPDATE saga_instances
SET current_state = $1, version = version + 1, updated_at = $2
WHERE tenant_id = $3 AND id = $4 AND version = $5 AND fencing_token = $6;`

	AcquireNextSagaLease = `
UPDATE saga_instances
SET lease_owner = $1, lease_until = $2, fencing_token = fencing_token + 1, updated_at = $3
WHERE id = (
    SELECT id FROM saga_instances
    WHERE current_state IN ('CREATED', 'RUNNING', 'ROLLING_FORWARD', 'COMPENSATING')
      AND (lease_owner = '' OR lease_until IS NULL OR lease_until < $3 OR lease_owner = $1)
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, public_id, tenant_id, workflow_definition_id, workflow_version,
          spec_hash, source, external_id, input_hash, correlation_id, current_state,
          version, fencing_token, lease_owner, lease_until, deadline_at,
          initial_payload, final_output, created_at, updated_at;`

	ReleaseSagaLease = `
UPDATE saga_instances
SET lease_owner = '', lease_until = NULL, updated_at = $1
WHERE tenant_id = $2 AND id = $3 AND fencing_token = $4;`

	AcquireSagaLeaseByID = `
UPDATE saga_instances
SET lease_owner = $1, lease_until = $2, fencing_token = fencing_token + 1, updated_at = $3
WHERE tenant_id = $4 AND id = $5
RETURNING id, public_id, tenant_id, workflow_definition_id, workflow_version,
          spec_hash, source, external_id, input_hash, correlation_id, current_state,
          version, fencing_token, lease_owner, lease_until, deadline_at,
          initial_payload, final_output, created_at, updated_at;`

	UpdateStepExecution = `
UPDATE saga_step_executions
SET execution_state = $1, attempt_count = $2, step_output = $3,
    error_message = $4, completed_at = $5, updated_at = $6
WHERE tenant_id = $7 AND saga_id = $8 AND step_id = $9;`

	ListSagasByTenant = `
SELECT id, public_id, tenant_id, workflow_definition_id, workflow_version,
       spec_hash, source, external_id, input_hash, correlation_id, current_state,
       version, fencing_token, lease_owner, lease_until, deadline_at,
       initial_payload, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;`
)

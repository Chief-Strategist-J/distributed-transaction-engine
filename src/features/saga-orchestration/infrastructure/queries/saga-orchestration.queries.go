package queries

const (
	InsertSagaInstance = `
INSERT INTO saga_instances (
    id, public_id, tenant_id, workflow_definition_id, workflow_version,
    spec_hash, source, external_id, correlation_id, current_state,
    version, fencing_token, initial_payload, final_output, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);`

	InsertSagaStepExecution = `
INSERT INTO saga_step_executions (
    id, public_id, tenant_id, saga_id, step_id,
    execution_state, attempt_count, step_input, step_output,
    error_message, idempotency_key, started_at, completed_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);`

	SelectSagaByID = `
SELECT id, public_id, tenant_id, workflow_definition_id, workflow_version,
       spec_hash, source, external_id, correlation_id, current_state,
       version, fencing_token, initial_payload, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1 AND id = $2;`

	SelectSagaByPublicID = `
SELECT id, public_id, tenant_id, workflow_definition_id, workflow_version,
       spec_hash, source, external_id, correlation_id, current_state,
       version, fencing_token, initial_payload, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1 AND public_id = $2;`

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

	UpdateStepExecution = `
UPDATE saga_step_executions
SET execution_state = $1, attempt_count = $2, step_output = $3,
    error_message = $4, completed_at = $5, updated_at = $6
WHERE tenant_id = $7 AND saga_id = $8 AND step_id = $9;`

	ListSagasByTenant = `
SELECT id, public_id, tenant_id, workflow_definition_id, workflow_version,
       spec_hash, source, external_id, correlation_id, current_state,
       version, fencing_token, initial_payload, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;`
)

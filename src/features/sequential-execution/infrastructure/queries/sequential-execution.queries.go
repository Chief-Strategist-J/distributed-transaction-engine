package queries

const (
	FLOW_CREATE_SEQUENTIAL_SAGA = `
INSERT INTO saga_instances (
    id, public_id, tenant_id, workflow_definition_id, workflow_version,
    spec_hash, source, external_id, input_hash, correlation_id,
    current_state, version, current_step_index, total_steps, execution_phase,
    pivot_reached, owner_epoch, deadline_at, initial_payload, final_output,
    created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15,
    $16, $17, $18, $19, $20,
    $21, $22
);`

	FLOW_GET_SEQUENTIAL_SAGA = `
SELECT id, public_id, tenant_id, workflow_definition_id, current_state,
       current_step_index, total_steps, execution_phase, pivot_reached,
       version, owner_epoch, final_output, created_at, updated_at
FROM saga_instances
WHERE tenant_id = $1 AND id = $2;`

	FLOW_UPDATE_SAGA_CAS = `
UPDATE saga_instances
SET current_state = $1,
    current_step_index = $2,
    execution_phase = $3,
    pivot_reached = $4,
    final_output = $5,
    version = version + 1,
    updated_at = NOW()
WHERE tenant_id = $6
  AND id = $7
  AND version = $8
  AND owner_epoch = $9;`

	FLOW_INSERT_CONTEXT_ENTRY = `
INSERT INTO saga_context_entries (
    id, public_id, tenant_id, saga_id, producer_step_index,
    entry_name, entry_value, schema_version, correlation_id,
    source, external_id, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, 1, $8,
    'SYSTEM', $9, 1, NOW(), NOW()
);`

	FLOW_GET_CONTEXT_ENTRIES = `
SELECT id, public_id, tenant_id, saga_id, producer_step_index,
       entry_name, entry_value, correlation_id
FROM saga_context_entries
WHERE tenant_id = $1 AND saga_id = $2
ORDER BY producer_step_index ASC;`
)

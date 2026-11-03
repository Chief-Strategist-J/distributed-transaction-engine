package queries

const (
	FlowInsertWorkflowDefinition = `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, status, raw_spec, total_steps, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)
	`

	FlowInsertWorkflowDefinitionStep = `
		INSERT INTO workflow_definition_steps (
			id, public_id, tenant_id, workflow_id, step_id, step_name, is_pivot,
			action_type, action_endpoint, compensation_action, compensation_endpoint,
			timeout_seconds, max_retries, dependencies, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
	`

	FlowSelectWorkflowDefinitionByID = `
		SELECT 
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, status, raw_spec, total_steps, created_at, updated_at
		FROM workflow_definitions
		WHERE tenant_id = $1 AND id = $2
	`

	FlowSelectWorkflowDefinitionByNameAndVersion = `
		SELECT 
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, status, raw_spec, total_steps, created_at, updated_at
		FROM workflow_definitions
		WHERE tenant_id = $1 AND name = $2 AND version = $3
	`

	FlowSelectWorkflowDefinitionByHash = `
		SELECT 
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, status, raw_spec, total_steps, created_at, updated_at
		FROM workflow_definitions
		WHERE tenant_id = $1 AND name = $2 AND spec_hash = $3
	`

	FlowSelectWorkflowDefinitionSteps = `
		SELECT 
			id, public_id, tenant_id, workflow_id, step_id, step_name, is_pivot,
			action_type, action_endpoint, compensation_action, compensation_endpoint,
			timeout_seconds, max_retries, dependencies, created_at, updated_at
		FROM workflow_definition_steps
		WHERE tenant_id = $1 AND workflow_id = $2
		ORDER BY created_at ASC
	`

	FlowListWorkflowDefinitionsByTenant = `
		SELECT 
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, status, raw_spec, total_steps, created_at, updated_at
		FROM workflow_definitions
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
)


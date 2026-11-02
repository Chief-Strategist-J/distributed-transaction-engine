package queries

const (
	FlowInsertSagaState = `
		INSERT INTO saga_instance_states (
			id, public_id, tenant_id, source, external_id, correlation_id, 
			current_state, version, fencing_token, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`

	FlowSelectSagaState = `
		SELECT 
			id, public_id, tenant_id, source, external_id, correlation_id, 
			current_state, version, fencing_token, created_at, updated_at
		FROM saga_instance_states
		WHERE tenant_id = $1 AND id = $2
	`

	FlowUpdateSagaStateCAS = `
		UPDATE saga_instance_states
		SET current_state = $1, version = version + 1, fencing_token = $2, updated_at = $3
		WHERE tenant_id = $4 AND id = $5 AND version = $6 AND current_state = $7
		RETURNING 
			id, public_id, tenant_id, source, external_id, correlation_id, 
			current_state, version, fencing_token, created_at, updated_at
	`

	FlowInsertStateAudit = `
		INSERT INTO state_transition_audit (
			id, public_id, tenant_id, saga_id, correlation_id, 
			from_state, to_state, version_before, version_after, 
			fencing_token, reason, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`

	FlowSelectStateAudits = `
		SELECT 
			id, public_id, tenant_id, saga_id, correlation_id, 
			from_state, to_state, version_before, version_after, 
			fencing_token, reason, created_at, updated_at
		FROM state_transition_audit
		WHERE tenant_id = $1 AND saga_id = $2
		ORDER BY created_at ASC
	`

	FlowSelectAllowedTransitions = `
		SELECT from_state, to_state
		FROM saga_state_transitions
	`

	FlowCheckSagaOrphanAudits = `
		SELECT a.tenant_id, a.id AS audit_id, a.saga_id
		FROM state_transition_audit a
		LEFT JOIN saga_instance_states s ON a.tenant_id = s.tenant_id AND a.saga_id = s.id
		WHERE s.id IS NULL
	`

	FlowCheckSagaTerminalNoForwardTransitions = `
		SELECT a.tenant_id, a.saga_id, a.from_state, a.to_state
		FROM state_transition_audit a
		WHERE a.from_state IN ('COMPLETED', 'CANCELLED', 'COMPENSATED', 'FAILED')
	`
)

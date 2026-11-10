package queries

const (
	FLOW_INSERT_MANUAL_INTERVENTION_AUDIT = `
INSERT INTO manual_intervention_audit_log (
    id, public_id, tenant_id, source, external_id, correlation_id, saga_id,
    operator_id, operator_role, action_type, reason, approval_ticket_id,
    synthetic_payload, old_state, new_state, version, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
);
`

	FLOW_SELECT_MANUAL_INTERVENTIONS_BY_SAGA = `
SELECT 
    id, public_id, tenant_id, source, external_id, correlation_id, saga_id,
    operator_id, operator_role, action_type, reason, approval_ticket_id,
    synthetic_payload, old_state, new_state, version, created_at
FROM manual_intervention_audit_log
WHERE tenant_id = $1 AND saga_id = $2
ORDER BY created_at DESC;
`

	FLOW_SELECT_MANUAL_INTERVENTIONS_BY_OPERATOR = `
SELECT 
    id, public_id, tenant_id, source, external_id, correlation_id, saga_id,
    operator_id, operator_role, action_type, reason, approval_ticket_id,
    synthetic_payload, old_state, new_state, version, created_at
FROM manual_intervention_audit_log
WHERE tenant_id = $1 AND operator_id = $2
ORDER BY created_at DESC;
`
)

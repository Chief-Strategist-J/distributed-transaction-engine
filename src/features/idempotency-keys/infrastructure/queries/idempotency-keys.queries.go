package queries

const (
	FLOW_INSERT_IDEMPOTENCY_KEY = `
INSERT INTO idempotency_keys (
    id, public_id, tenant_id, source, external_id, correlation_id,
    principal_id, operation_id, idempotency_key, request_fingerprint,
    current_state, lease_owner, lease_until, recovery_fencing_token,
    created_at, updated_at, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10,
    $11, $12, $13, $14,
    $15, $16, $17
)
ON CONFLICT (tenant_id, principal_id, operation_id, idempotency_key) DO NOTHING
RETURNING id;
`

	FLOW_GET_BY_SCOPE = `
SELECT id, public_id, tenant_id, source, external_id, correlation_id,
       principal_id, operation_id, idempotency_key, request_fingerprint,
       current_state, response_status, response_headers, response_body,
       resource_reference, lease_owner, lease_until, recovery_fencing_token,
       created_at, updated_at, completed_at, expires_at
FROM idempotency_keys
WHERE tenant_id = $1 
  AND principal_id = $2 
  AND operation_id = $3 
  AND idempotency_key = $4;
`

	FLOW_COMMIT_OUTCOME = `
UPDATE idempotency_keys
SET current_state = $1,
    response_status = $2,
    response_headers = $3,
    response_body = $4,
    resource_reference = $5,
    completed_at = $6,
    updated_at = $7
WHERE tenant_id = $8 
  AND id = $9;
`
)


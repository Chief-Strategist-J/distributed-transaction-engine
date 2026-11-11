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

	FLOW_TAKEOVER_EXPIRED_LEASE = `
UPDATE idempotency_keys
SET lease_owner = $1,
    lease_until = $2,
    recovery_fencing_token = recovery_fencing_token + 1,
    updated_at = $3
WHERE tenant_id = $4
  AND principal_id = $5
  AND operation_id = $6
  AND idempotency_key = $7
  AND request_fingerprint = $8
  AND current_state = 'IN_PROGRESS'
  AND ((lease_until IS NOT NULL AND lease_until < $3) OR (lease_until IS NULL AND updated_at < $9))
RETURNING recovery_fencing_token;
`
)

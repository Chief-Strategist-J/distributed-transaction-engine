package queries

const (
	FLOW_INSERT_STEP_RETRY = `
INSERT INTO step_retries (
    id, public_id, tenant_id, saga_id, step_id, attempt_number,
    error_class, error_message, error_fingerprint, retry_after_seconds,
    computed_delay_millis, next_attempt_at, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10,
    $11, $12, $13
)
ON CONFLICT (tenant_id, saga_id, step_id, attempt_number) DO NOTHING;
`

	FLOW_GET_RETRY_POLICY = `
SELECT id, public_id, tenant_id, policy_name, max_attempts,
       max_elapsed_seconds, base_interval_millis, max_interval_millis,
       backoff_algorithm
FROM retry_policies
WHERE tenant_id = $1 AND policy_name = $2;
`

	FLOW_UPSERT_RETRY_POLICY = `
INSERT INTO retry_policies (
    id, public_id, tenant_id, source, external_id, correlation_id,
    policy_name, max_attempts, max_elapsed_seconds, base_interval_millis,
    max_interval_millis, backoff_algorithm, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10,
    $11, $12, $13, $14
)
ON CONFLICT (tenant_id, policy_name) DO UPDATE SET
    max_attempts = EXCLUDED.max_attempts,
    max_elapsed_seconds = EXCLUDED.max_elapsed_seconds,
    base_interval_millis = EXCLUDED.base_interval_millis,
    max_interval_millis = EXCLUDED.max_interval_millis,
    backoff_algorithm = EXCLUDED.backoff_algorithm,
    updated_at = EXCLUDED.updated_at;
`
)


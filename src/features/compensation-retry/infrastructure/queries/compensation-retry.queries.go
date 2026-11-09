package queries

const (
	QueryUpsertCompensationRetry = `
		INSERT INTO compensation_retries (
			id, public_id, tenant_id, source, external_id, correlation_id,
			saga_id, step_id, step_type, dependency_id,
			attempt_number, max_attempts, first_attempt_at, business_deadline,
			next_retry_at, status, last_error, version, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, 1, NOW(), NOW()
		)
		ON CONFLICT (tenant_id, saga_id, step_id) DO UPDATE
		SET attempt_number = EXCLUDED.attempt_number,
		    next_retry_at = EXCLUDED.next_retry_at,
		    status = EXCLUDED.status,
		    last_error = EXCLUDED.last_error,
		    version = compensation_retries.version + 1,
		    updated_at = NOW()
		RETURNING id, public_id, tenant_id, source, external_id, correlation_id,
		          saga_id, step_id, step_type, dependency_id,
		          attempt_number, max_attempts, first_attempt_at, business_deadline,
		          next_retry_at, status, last_error, version, created_at, updated_at;
	`

	QueryUpdateCompensationStatus = `
		UPDATE compensation_retries
		SET status = $1,
		    next_retry_at = $2,
		    last_error = $3,
		    version = version + 1,
		    updated_at = NOW()
		WHERE tenant_id = $4 AND saga_id = $5 AND step_id = $6;
	`

	QueryGetCompensationRetry = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       saga_id, step_id, step_type, dependency_id,
		       attempt_number, max_attempts, first_attempt_at, business_deadline,
		       next_retry_at, status, last_error, version, created_at, updated_at
		FROM compensation_retries
		WHERE tenant_id = $1 AND saga_id = $2 AND step_id = $3;
	`

	QueryUpsertStepTypePause = `
		INSERT INTO compensation_step_type_pauses (
			id, public_id, tenant_id, source, external_id, correlation_id,
			step_type, is_paused, reason, version, created_at, updated_at
		) VALUES (
			gen_random_uuid(), $1, $2, 'ENGINE_COMPENSATION', $3, gen_random_uuid(),
			$4, $5, $6, 1, NOW(), NOW()
		)
		ON CONFLICT (tenant_id, step_type) DO UPDATE
		SET is_paused = EXCLUDED.is_paused,
		    reason = EXCLUDED.reason,
		    version = compensation_step_type_pauses.version + 1,
		    updated_at = NOW();
	`

	QueryGetStepTypePause = `
		SELECT is_paused
		FROM compensation_step_type_pauses
		WHERE tenant_id = $1 AND step_type = $2;
	`

	FlowCheckCompensationRetryOverdue = `
		SELECT tenant_id, id, public_id, business_deadline, status
		FROM compensation_retries
		WHERE status IN ('PENDING', 'RETRY_SCHEDULED', 'VERIFYING_AMBIGUOUS')
		  AND business_deadline < NOW();
	`

	FlowCheckCompensationRetryInvalidTransition = `
		SELECT r.tenant_id, r.id, r.status
		FROM compensation_retries r
		LEFT JOIN compensation_retry_transitions t ON t.to_state = r.status
		WHERE t.to_state IS NULL;
	`
)

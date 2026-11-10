package queries

const (
	QueryInsertDeadLetterRecord = `
		INSERT INTO dead_letter_records (
			id, public_id, tenant_id, source, external_id, correlation_id,
			saga_id, step_index, step_type, attempt_id, idempotency_key,
			outcome_class, failure_class, error_fingerprint, sanitized_message,
			payload_ref, schema_version, money_at_stake_cents, currency,
			business_deadline, status, version, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13, $14, $15,
			$16, $17, $18, $19,
			$20, 'QUARANTINED', 1, NOW(), NOW()
		)
		RETURNING id, public_id, tenant_id, source, external_id, correlation_id,
		          saga_id, step_index, step_type, attempt_id, idempotency_key,
		          outcome_class, failure_class, error_fingerprint, sanitized_message,
		          payload_ref, schema_version, money_at_stake_cents, currency,
		          business_deadline, status, disposition, actor_id, second_approver_id,
		          disposition_reason, evidence_ref, corrective_attempt_id, corrective_idempotency_key,
		          version, created_at, updated_at;
	`

	QueryUpdateSagaNeedsAttention = `
		UPDATE saga_instance_states
		SET current_state = 'NEEDS_ATTENTION',
		    version = version + 1,
		    updated_at = NOW()
		WHERE tenant_id = $1
		  AND id = $2
		  AND current_state <> 'COMPENSATED'
		  AND current_state <> 'COMPLETED';
	`

	QueryInsertOutboxNotification = `
		INSERT INTO outbox_events (
			id, public_id, tenant_id, saga_id,
			aggregate_type, aggregate_id, aggregate_sequence,
			destination_topic, partition_key, event_type,
			payload, headers, status, version, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4,
			'DEAD_LETTER_RECORD', $5, $6,
			$7, $8, 'DEAD_LETTER_QUARANTINED',
			$9::jsonb, '{}'::jsonb, 'PENDING', 1, NOW(), NOW()
		);
	`

	QueryGetRecordByID = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       saga_id, step_index, step_type, attempt_id, idempotency_key,
		       outcome_class, failure_class, error_fingerprint, sanitized_message,
		       payload_ref, schema_version, money_at_stake_cents, currency,
		       business_deadline, status, disposition, actor_id, second_approver_id,
		       disposition_reason, evidence_ref, corrective_attempt_id, corrective_idempotency_key,
		       version, created_at, updated_at
		FROM dead_letter_records
		WHERE tenant_id = $1 AND id = $2;
	`

	QueryUpdateRecordStatus = `
		UPDATE dead_letter_records
		SET status = $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE tenant_id = $2
		  AND id = $3
		  AND version = $4
		RETURNING id, public_id, tenant_id, source, external_id, correlation_id,
		          saga_id, step_index, step_type, attempt_id, idempotency_key,
		          outcome_class, failure_class, error_fingerprint, sanitized_message,
		          payload_ref, schema_version, money_at_stake_cents, currency,
		          business_deadline, status, disposition, actor_id, second_approver_id,
		          disposition_reason, evidence_ref, corrective_attempt_id, corrective_idempotency_key,
		          version, created_at, updated_at;
	`

	QueryResolveRecord = `
		UPDATE dead_letter_records
		SET status = 'RESOLVED',
		    disposition = $1,
		    actor_id = $2,
		    second_approver_id = $3,
		    disposition_reason = $4,
		    evidence_ref = $5,
		    corrective_attempt_id = $6,
		    corrective_idempotency_key = $7,
		    version = version + 1,
		    updated_at = NOW()
		WHERE tenant_id = $8
		  AND id = $9
		  AND version = $10
		RETURNING id, public_id, tenant_id, source, external_id, correlation_id,
		          saga_id, step_index, step_type, attempt_id, idempotency_key,
		          outcome_class, failure_class, error_fingerprint, sanitized_message,
		          payload_ref, schema_version, money_at_stake_cents, currency,
		          business_deadline, status, disposition, actor_id, second_approver_id,
		          disposition_reason, evidence_ref, corrective_attempt_id, corrective_idempotency_key,
		          version, created_at, updated_at;
	`

	QueryCountRecentFingerprint = `
		SELECT COUNT(*)
		FROM dead_letter_records
		WHERE tenant_id = $1
		  AND error_fingerprint = $2
		  AND created_at >= $3;
	`

	QueryInsertSystemicIncident = `
		INSERT INTO dlq_systemic_incidents (
			id, public_id, tenant_id, source, external_id, correlation_id,
			fingerprint, step_type, incident_status, affected_count,
			paused_at, owner_team, review_deadline, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, 'ACTIVE', $9,
			NOW(), $10, $11, NOW(), NOW()
		)
		ON CONFLICT (tenant_id, public_id) DO NOTHING;
	`

	QueryInsertStepTypePause = `
		INSERT INTO dlq_step_type_pauses (
			tenant_id, step_type, incident_id, paused_at, review_deadline, owner_team
		) VALUES (
			$1, $2, $3, NOW(), $4, $5
		)
		ON CONFLICT (tenant_id, step_type) DO UPDATE
		SET review_deadline = EXCLUDED.review_deadline,
		    paused_at = NOW();
	`

	QueryCheckStepTypePaused = `
		SELECT COUNT(*)
		FROM dlq_step_type_pauses
		WHERE tenant_id = $1 AND step_type = $2;
	`

	QueryListRecords = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       saga_id, step_index, step_type, attempt_id, idempotency_key,
		       outcome_class, failure_class, error_fingerprint, sanitized_message,
		       payload_ref, schema_version, money_at_stake_cents, currency,
		       business_deadline, status, disposition, actor_id, second_approver_id,
		       disposition_reason, evidence_ref, corrective_attempt_id, corrective_idempotency_key,
		       version, created_at, updated_at
		FROM dead_letter_records
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2;
	`

	QueryListRecordsByStatus = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       saga_id, step_index, step_type, attempt_id, idempotency_key,
		       outcome_class, failure_class, error_fingerprint, sanitized_message,
		       payload_ref, schema_version, money_at_stake_cents, currency,
		       business_deadline, status, disposition, actor_id, second_approver_id,
		       disposition_reason, evidence_ref, corrective_attempt_id, corrective_idempotency_key,
		       version, created_at, updated_at
		FROM dead_letter_records
		WHERE tenant_id = $1 AND status = $2
		ORDER BY created_at DESC
		LIMIT $3;
	`
)


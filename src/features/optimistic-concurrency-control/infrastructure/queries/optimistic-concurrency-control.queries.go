package queries

const (
	QueryExecuteCAS = `
		UPDATE saga_instance_states
		SET current_state = $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE tenant_id = $2
		  AND id = $3
		  AND version = $4
		  AND owner_epoch = $5
		RETURNING id, public_id, tenant_id, current_state, version, owner_epoch, updated_at;
	`

	QueryDiagnoseState = `
		SELECT id, public_id, tenant_id, current_state, version, owner_epoch, updated_at
		FROM saga_instance_states
		WHERE tenant_id = $1 AND id = $2;
	`

	QueryParkPendingInput = `
		INSERT INTO occ_pending_inputs (
			id, public_id, tenant_id, source, external_id, correlation_id,
			saga_id, input_type, input_payload, attempt_count, last_conflict_reason,
			status, version, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9::jsonb, $10, $11,
			'PENDING', 1, NOW(), NOW()
		)
		ON CONFLICT (tenant_id, public_id) DO NOTHING;
	`

	QueryRecordContention = `
		INSERT INTO occ_contention_journal (
			id, public_id, tenant_id, saga_id, attempted_version, current_version,
			attempted_epoch, current_epoch, diagnosis, worker_id, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, NOW()
		);
	`

	QueryCountRecentContention = `
		SELECT COUNT(*)
		FROM occ_contention_journal
		WHERE tenant_id = $1
		  AND saga_id = $2
		  AND created_at >= $3;
	`

	QueryInsertOutbox = `
		INSERT INTO outbox_events (
			id, public_id, tenant_id, saga_id,
			aggregate_type, aggregate_id, aggregate_sequence,
			destination_topic, partition_key, event_type,
			payload, headers, status, version, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4,
			'SAGA_OCC', $5, $6,
			$7, $8, 'SAGA_STATE_TRANSITIONED',
			$9::jsonb, '{}'::jsonb, 'PENDING', 1, NOW(), NOW()
		);
	`

	QuerySelectInputApplied = `
		SELECT EXISTS (
			SELECT 1 FROM inbox_messages
			WHERE tenant_id = $1 AND handler_name = 'OCC_APPLIED' AND handler_generation = 1 AND external_id = $2
		);
	`

	QueryInsertInputApplied = `
		INSERT INTO inbox_messages (
			id, public_id, tenant_id, source, external_id, correlation_id, handler_name, handler_generation,
			content_hash, status, attempt_count, producer_event_time, payload, version, created_at, updated_at
		) VALUES (
			$1, $2, $3, 'ENGINE_OCC', $4, $5, 'OCC_APPLIED', 1, $6, 'APPLIED', 1, NOW(), '{}'::jsonb, 1, NOW(), NOW()
		)
		ON CONFLICT ON CONSTRAINT inbox_messages_dedup_uk DO NOTHING;
	`
)

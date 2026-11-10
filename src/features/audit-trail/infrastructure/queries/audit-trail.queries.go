package queries

const (
	FLOW_APPEND_OUTBOX = `
		INSERT INTO audit_outbox (
			id, public_id, tenant_id, source, external_id, correlation_id,
			audit_id, occurred_at, action, actor_id, actor_ip, on_behalf_of,
			target_resource, payload, status, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)`

	FLOW_GET_PENDING_OUTBOX = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       audit_id, occurred_at, action, actor_id, actor_ip, on_behalf_of,
		       target_resource, payload, status, created_at, chained_at
		FROM audit_outbox
		WHERE tenant_id = $1 AND status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $2`

	FLOW_MARK_OUTBOX_CHAINED = `
		UPDATE audit_outbox
		SET status = 'CHAINED', chained_at = $1
		WHERE tenant_id = $2 AND id = ANY($3)`

	FLOW_GET_TAIL_EVENT = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       audit_id, segment_id, sequence_id, key_id, actor_id, actor_ip,
		       action, prev_hash, curr_hash, raw_payload, occurred_at, created_at
		FROM immutable_audit_log
		WHERE tenant_id = $1 AND segment_id = $2
		ORDER BY sequence_id DESC
		LIMIT 1`

	FLOW_APPEND_CHAINED_EVENT = `
		INSERT INTO immutable_audit_log (
			id, public_id, tenant_id, source, external_id, correlation_id,
			audit_id, segment_id, sequence_id, key_id, actor_id, actor_ip,
			action, prev_hash, curr_hash, raw_payload, occurred_at, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
		)`

	FLOW_GET_EVENTS_BY_SEGMENT = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       audit_id, segment_id, sequence_id, key_id, actor_id, actor_ip,
		       action, prev_hash, curr_hash, raw_payload, occurred_at, created_at
		FROM immutable_audit_log
		WHERE tenant_id = $1 AND segment_id = $2
		ORDER BY sequence_id ASC
		LIMIT $3`

	FLOW_GET_EVENTS_BY_TENANT = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       audit_id, segment_id, sequence_id, key_id, actor_id, actor_ip,
		       action, prev_hash, curr_hash, raw_payload, occurred_at, created_at
		FROM immutable_audit_log
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2`

	FLOW_CHECK_AUDIT_ID_CHAINED = `
		SELECT EXISTS (
			SELECT 1 FROM immutable_audit_log
			WHERE tenant_id = $1 AND audit_id = $2
		)`

	FLOW_APPEND_QUARANTINE = `
		INSERT INTO audit_quarantine (
			id, public_id, tenant_id, source, external_id, correlation_id,
			audit_id, quarantine_reason, raw_record, error_details, resolved, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)`

	FLOW_GET_QUARANTINE_EVENTS = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       audit_id, quarantine_reason, raw_record, error_details, resolved, created_at
		FROM audit_quarantine
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2`

	FLOW_SAVE_CHECKPOINT = `
		INSERT INTO audit_checkpoints (
			id, public_id, tenant_id, source, external_id, correlation_id,
			segment_id, last_sequence_id, head_hash, signature, key_id, event_count, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		)`

	FLOW_GET_LATEST_CHECKPOINT = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       segment_id, last_sequence_id, head_hash, signature, key_id, event_count, created_at
		FROM audit_checkpoints
		WHERE tenant_id = $1 AND segment_id = $2
		ORDER BY created_at DESC
		LIMIT 1`

	FLOW_RECORD_VERIFICATION = `
		INSERT INTO audit_chain_verifications (
			id, public_id, tenant_id, source, external_id, correlation_id,
			segment_id, verified_up_to_sequence, total_events_checked,
			status, failure_sequence_id, failure_reason, verified_by, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)`
)

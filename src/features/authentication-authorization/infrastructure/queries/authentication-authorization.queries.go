package queries

const (
	FLOW_SAVE_API_KEY = `
		INSERT INTO api_keys (
			id, public_id, tenant_id, source, external_id, correlation_id,
			key_prefix, key_hash, name, scopes, allowed_cidrs, status, expires_at, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)`

	FLOW_GET_API_KEY_BY_PREFIX = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       key_prefix, key_hash, name, scopes, allowed_cidrs, status, expires_at, created_at
		FROM api_keys
		WHERE tenant_id = $1 AND key_prefix = $2
		LIMIT 1`

	FLOW_UPDATE_API_KEY_STATUS = `
		UPDATE api_keys
		SET status = $1
		WHERE tenant_id = $2 AND id = $3`

	FLOW_SAVE_SERVICE_IDENTITY = `
		INSERT INTO service_identities (
			id, public_id, tenant_id, source, external_id, correlation_id,
			spiffe_id, trust_domain, allowed_methods, status, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)`

	FLOW_GET_SERVICE_IDENTITY = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       spiffe_id, trust_domain, allowed_methods, status, created_at
		FROM service_identities
		WHERE tenant_id = $1 AND spiffe_id = $2
		LIMIT 1`

	FLOW_SAVE_AUTH_DECISION = `
		INSERT INTO auth_decision_audit (
			id, public_id, tenant_id, source, external_id, correlation_id,
			caller_type, principal_id, action, resource_id, decision,
			reason_code, risk_level, step_up_verified, approver_id, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)`

	FLOW_GET_DECISIONS_BY_PRINCIPAL = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       caller_type, principal_id, action, resource_id, decision,
		       reason_code, risk_level, step_up_verified, approver_id, created_at
		FROM auth_decision_audit
		WHERE tenant_id = $1 AND principal_id = $2
		ORDER BY created_at DESC
		LIMIT $3`

	FLOW_SAVE_REVOCATION = `
		INSERT INTO revocation_records (
			id, public_id, tenant_id, source, external_id, correlation_id,
			target_type, target_id, revocation_reason, revoked_at, expires_at, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)`

	FLOW_CHECK_REVOCATION = `
		SELECT EXISTS (
			SELECT 1 FROM revocation_records
			WHERE tenant_id = $1 AND target_type = $2 AND target_id = $3 AND expires_at > NOW()
		)`

	FLOW_SAVE_REPLAY_NONCE = `
		INSERT INTO provider_replay_cache (
			id, public_id, tenant_id, source, external_id, correlation_id,
			provider, nonce, received_at, expires_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)`

	FLOW_CHECK_REPLAY_NONCE = `
		SELECT EXISTS (
			SELECT 1 FROM provider_replay_cache
			WHERE tenant_id = $1 AND provider = $2 AND nonce = $3 AND expires_at > NOW()
		)`
)


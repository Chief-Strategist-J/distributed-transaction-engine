package queries

const (
	FLOW_INSERT_BUNDLE = `
		INSERT INTO workflow_bundles (
			id, public_id, tenant_id, source, external_id, correlation_id,
			workflow_name, version_tag, content_hash, signature, status,
			bundle_payload, capabilities_required, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

	FLOW_SELECT_BUNDLE_BY_ID = `
		SELECT
			id, public_id, tenant_id, source, external_id, correlation_id,
			workflow_name, version_tag, content_hash, signature, status,
			bundle_payload, capabilities_required, created_at, updated_at
		FROM workflow_bundles
		WHERE tenant_id = $1 AND id = $2`

	FLOW_SELECT_BUNDLE_BY_VERSION = `
		SELECT
			id, public_id, tenant_id, source, external_id, correlation_id,
			workflow_name, version_tag, content_hash, signature, status,
			bundle_payload, capabilities_required, created_at, updated_at
		FROM workflow_bundles
		WHERE tenant_id = $1 AND workflow_name = $2 AND version_tag = $3`

	FLOW_SELECT_ACTIVE_BUNDLE = `
		SELECT
			id, public_id, tenant_id, source, external_id, correlation_id,
			workflow_name, version_tag, content_hash, signature, status,
			bundle_payload, capabilities_required, created_at, updated_at
		FROM workflow_bundles
		WHERE tenant_id = $1 AND workflow_name = $2 AND status = 'ACTIVE'
		ORDER BY created_at DESC
		LIMIT 1`

	FLOW_UPDATE_BUNDLE_STATUS = `
		UPDATE workflow_bundles
		SET status = $1, updated_at = NOW()
		WHERE tenant_id = $2 AND id = $3`

	FLOW_INSERT_PROMOTION = `
		INSERT INTO workflow_version_promotions (
			id, public_id, tenant_id, source, external_id, correlation_id,
			bundle_id, action, proposer_id, approver_id, status, reason,
			decided_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

	FLOW_SELECT_PROMOTION_BY_ID = `
		SELECT
			id, public_id, tenant_id, source, external_id, correlation_id,
			bundle_id, action, proposer_id, approver_id, status, reason,
			decided_at, created_at, updated_at
		FROM workflow_version_promotions
		WHERE tenant_id = $1 AND id = $2`

	FLOW_UPDATE_PROMOTION_DECISION = `
		UPDATE workflow_version_promotions
		SET status = $1, approver_id = $2, reason = $3, decided_at = $4, updated_at = NOW()
		WHERE tenant_id = $5 AND id = $6`

	FLOW_SELECT_ENTITY_COHORT = `
		SELECT
			id, public_id, tenant_id, source, external_id, correlation_id,
			workflow_name, entity_id, cohort_bucket, bundle_id, assigned_at
		FROM workflow_entity_cohorts
		WHERE tenant_id = $1 AND workflow_name = $2 AND entity_id = $3`

	FLOW_INSERT_ENTITY_COHORT = `
		INSERT INTO workflow_entity_cohorts (
			id, public_id, tenant_id, source, external_id, correlation_id,
			workflow_name, entity_id, cohort_bucket, bundle_id, assigned_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (tenant_id, workflow_name, entity_id) DO NOTHING`

	FLOW_INSERT_MIGRATION = `
		INSERT INTO workflow_version_migrations (
			id, public_id, tenant_id, source, external_id, correlation_id,
			saga_id, from_bundle_id, to_bundle_id, status, safe_point_step_id,
			state_mapping_applied, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

	FLOW_SELECT_MIGRATION_BY_ID = `
		SELECT
			id, public_id, tenant_id, source, external_id, correlation_id,
			saga_id, from_bundle_id, to_bundle_id, status, safe_point_step_id,
			state_mapping_applied, created_at, updated_at
		FROM workflow_version_migrations
		WHERE tenant_id = $1 AND id = $2`

	FLOW_UPDATE_MIGRATION_STATUS = `
		UPDATE workflow_version_migrations
		SET status = $1, updated_at = NOW()
		WHERE tenant_id = $2 AND id = $3`
)

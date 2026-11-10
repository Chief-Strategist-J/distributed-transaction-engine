package queries

const (
	tenantRegistryColumns = `id, public_id, tenant_id, correlation_id, status, suspension_reason,
    max_concurrent_sagas, status_changed_at, version, created_at, updated_at`

	tenantSagaQuotaColumns = `tenant_id, active_saga_count, version, created_at, updated_at`

	tenantQuarantineColumns = `id, public_id, correlation_id, source_topic, source_partition, source_offset,
    reason, status, envelope_tenant_id, key_tenant_id, payload_tenant_id, aggregate_tenant_id,
    payload_sha256, resolved_at, version, created_at, updated_at`
)

const (
	FLOW_SET_TENANT_CONTEXT = `
SELECT set_config('app.tenant_id', $1, true);
`

	FLOW_INSERT_TENANT_REGISTRY = `
INSERT INTO tenant_registry (
    id, public_id, tenant_id, correlation_id, status, suspension_reason,
    max_concurrent_sagas, status_changed_at, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
`

	FLOW_INSERT_TENANT_SAGA_QUOTA = `
INSERT INTO tenant_saga_quota (tenant_id, active_saga_count, version, created_at, updated_at)
VALUES ($1, 0, 1, $2, $2);
`

	FLOW_SELECT_TENANT_REGISTRY_BY_TENANT = `
SELECT ` + tenantRegistryColumns + `
FROM tenant_registry
WHERE tenant_id = $1;
`

	FLOW_UPDATE_TENANT_REGISTRY_STATUS = `
UPDATE tenant_registry
SET status = $3, suspension_reason = $4, version = version + 1
WHERE tenant_id = $1 AND version = $2
RETURNING ` + tenantRegistryColumns + `;
`

	FLOW_SELECT_TENANT_SAGA_QUOTA = `
SELECT ` + tenantSagaQuotaColumns + `
FROM tenant_saga_quota
WHERE tenant_id = $1;
`

	FLOW_RESERVE_TENANT_SAGA_SLOT = `
UPDATE tenant_saga_quota AS quota
SET active_saga_count = quota.active_saga_count + 1, version = quota.version + 1
FROM tenant_registry AS registry
WHERE quota.tenant_id = $1
  AND registry.tenant_id = quota.tenant_id
  AND registry.status = $2
  AND quota.active_saga_count < registry.max_concurrent_sagas
RETURNING quota.active_saga_count, registry.max_concurrent_sagas;
`

	FLOW_RELEASE_TENANT_SAGA_SLOT = `
UPDATE tenant_saga_quota
SET active_saga_count = active_saga_count - 1, version = version + 1
WHERE tenant_id = $1 AND active_saga_count > 0
RETURNING active_saga_count;
`

	FLOW_UPDATE_TENANT_SAGA_QUOTA_COUNTER = `
UPDATE tenant_saga_quota
SET active_saga_count = $3, version = version + 1
WHERE tenant_id = $1 AND version = $2
RETURNING ` + tenantSagaQuotaColumns + `;
`

	FLOW_INSERT_TENANT_MESSAGE_QUARANTINE = `
INSERT INTO tenant_message_quarantine (
    id, public_id, correlation_id, source_topic, source_partition, source_offset,
    reason, status, envelope_tenant_id, key_tenant_id, payload_tenant_id, aggregate_tenant_id,
    payload_sha256, resolved_at, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);
`

	FLOW_SELECT_TENANT_MESSAGE_QUARANTINE_BY_STATUS = `
SELECT ` + tenantQuarantineColumns + `
FROM tenant_message_quarantine
WHERE status = $1
ORDER BY created_at, id
LIMIT $2;
`

	FLOW_UPDATE_TENANT_MESSAGE_QUARANTINE_RELEASE_BY_TENANT = `
WITH picked AS (
    SELECT id AS picked_id
    FROM tenant_message_quarantine
    WHERE status = $1 AND reason = $2 AND envelope_tenant_id = $3
    ORDER BY created_at, id
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
UPDATE tenant_message_quarantine AS quarantine
SET status = $5, resolved_at = $6, version = quarantine.version + 1
FROM picked
WHERE quarantine.id = picked.picked_id
RETURNING ` + tenantQuarantineColumns + `;
`

	FLOW_INSERT_TENANT_SECURITY_EVENT = `
INSERT INTO tenant_security_event (
    id, public_id, actor_tenant_id, referenced_tenant_id, correlation_id,
    event_type, resource_kind, resource_id, occurred_at, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
`

	FLOW_CHECK_TENANT_REGISTRY_QUOTA_ROW_MISSING = `
SELECT registry.tenant_id
FROM tenant_registry AS registry
WHERE NOT EXISTS (SELECT 1 FROM tenant_saga_quota AS quota WHERE quota.tenant_id = registry.tenant_id);
`

	FLOW_CHECK_TENANT_SAGA_QUOTA_OVER_LIMIT = `
SELECT quota.tenant_id
FROM tenant_saga_quota AS quota
JOIN tenant_registry AS registry ON registry.tenant_id = quota.tenant_id
WHERE quota.active_saga_count > registry.max_concurrent_sagas;
`

	FLOW_CHECK_TENANT_REGISTRY_TOMBSTONE_MISSING = `
SELECT registry.tenant_id
FROM tenant_registry AS registry
WHERE registry.status = $1
  AND NOT EXISTS (SELECT 1 FROM tenant_tombstone AS tombstone WHERE tombstone.tenant_id = registry.tenant_id);
`
)

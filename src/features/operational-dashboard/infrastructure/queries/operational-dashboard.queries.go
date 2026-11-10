package queries

const (
	FLOW_CREATE_STREAM_SESSION = `
INSERT INTO dashboard_stream_sessions (
    id, public_id, tenant_id, source, external_id, correlation_id,
    principal_id, scope_type, impersonation_ticket, impersonation_expires_at,
    reauth_deadline, last_heartbeat_at, status, created_at, updated_at, version
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);`

	FLOW_GET_STREAM_SESSION = `
SELECT id, public_id, tenant_id, source, external_id, correlation_id,
       principal_id, scope_type, impersonation_ticket, impersonation_expires_at,
       reauth_deadline, last_heartbeat_at, status, created_at, updated_at, version
FROM dashboard_stream_sessions
WHERE tenant_id = $1 AND (id = $2 OR public_id = $2);`

	FLOW_UPDATE_STREAM_SESSION_STATUS = `
UPDATE dashboard_stream_sessions
SET status = $3, updated_at = NOW(), version = version + 1
WHERE tenant_id = $1 AND (id = $2 OR public_id = $2);`

	FLOW_TOUCH_HEARTBEAT = `
UPDATE dashboard_stream_sessions
SET last_heartbeat_at = $3, updated_at = NOW()
WHERE tenant_id = $1 AND (id = $2 OR public_id = $2);`

	FLOW_GET_TILE_SNAPSHOTS = `
SELECT id, public_id, tenant_id, source, external_id, correlation_id,
       tile_key, metrics_payload, data_age_ms, is_stale, as_of_version,
       source_timestamp, created_at, updated_at, version
FROM dashboard_tile_snapshots
WHERE tenant_id = $1 OR tenant_id = 'PLATFORM_WIDE'
ORDER BY tile_key ASC;`

	FLOW_UPSERT_TILE_SNAPSHOT = `
INSERT INTO dashboard_tile_snapshots (
    id, public_id, tenant_id, source, external_id, correlation_id,
    tile_key, metrics_payload, data_age_ms, is_stale, as_of_version,
    source_timestamp, created_at, updated_at, version
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (tenant_id, source, external_id)
DO UPDATE SET
    metrics_payload = EXCLUDED.metrics_payload,
    data_age_ms = EXCLUDED.data_age_ms,
    is_stale = EXCLUDED.is_stale,
    as_of_version = EXCLUDED.as_of_version,
    source_timestamp = EXCLUDED.source_timestamp,
    updated_at = NOW(),
    version = dashboard_tile_snapshots.version + 1;`

	FLOW_CREATE_ACTION_PREVIEW = `
INSERT INTO dashboard_action_previews (
    id, public_id, tenant_id, source, external_id, correlation_id,
    saga_id, action_type, snapshot_version, requires_second_approver,
    money_at_stake, preview_payload, status, expires_at, created_at, updated_at, version
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
);`

	FLOW_GET_ACTION_PREVIEW = `
SELECT id, public_id, tenant_id, source, external_id, correlation_id,
       saga_id, action_type, snapshot_version, requires_second_approver,
       money_at_stake, preview_payload, status, expires_at, created_at, updated_at, version
FROM dashboard_action_previews
WHERE tenant_id = $1 AND (id = $2 OR public_id = $2);`

	FLOW_UPDATE_ACTION_PREVIEW_STATUS = `
UPDATE dashboard_action_previews
SET status = $3, updated_at = NOW(), version = version + 1
WHERE tenant_id = $1 AND (id = $2 OR public_id = $2);`

	FLOW_RECORD_AUDIT_EVENT = `
INSERT INTO dashboard_audit_events (
    id, public_id, tenant_id, source, external_id, correlation_id,
    operator_id, event_type, resource_id, details, ip_address,
    created_at, updated_at, version
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
);`
)


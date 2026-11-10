package queries

const (
	FLOW_INSERT_RECONCILIATION_ANOMALY = `
INSERT INTO reconciliation_anomalies (
    id, public_id, tenant_id, source, external_id, correlation_id, saga_id,
    anomaly_type, status, internal_status, external_status,
    internal_amount_cents, external_amount_cents, currency,
    discrepancy_reason, resolution_action, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
)
ON CONFLICT (tenant_id, source, external_id) DO UPDATE SET
    status = EXCLUDED.status,
    anomaly_type = EXCLUDED.anomaly_type,
    discrepancy_reason = EXCLUDED.discrepancy_reason,
    resolution_action = EXCLUDED.resolution_action,
    updated_at = NOW(),
    version = reconciliation_anomalies.version + 1;
`

	FLOW_SELECT_RECONCILIATION_ANOMALY_BY_SAGA = `
SELECT 
    id, public_id, tenant_id, source, external_id, correlation_id, saga_id,
    anomaly_type, status, internal_status, external_status,
    internal_amount_cents, external_amount_cents, currency,
    discrepancy_reason, resolution_action, version, created_at, updated_at
FROM reconciliation_anomalies
WHERE tenant_id = $1 AND saga_id = $2
LIMIT 1;
`

	FLOW_SELECT_RECONCILIATION_ANOMALIES_BY_TENANT = `
SELECT 
    id, public_id, tenant_id, source, external_id, correlation_id, saga_id,
    anomaly_type, status, internal_status, external_status,
    internal_amount_cents, external_amount_cents, currency,
    discrepancy_reason, resolution_action, version, created_at, updated_at
FROM reconciliation_anomalies
WHERE tenant_id = $1
ORDER BY created_at DESC;
`

	FLOW_SELECT_RECONCILIATION_ANOMALIES_BY_TENANT_AND_STATUS = `
SELECT 
    id, public_id, tenant_id, source, external_id, correlation_id, saga_id,
    anomaly_type, status, internal_status, external_status,
    internal_amount_cents, external_amount_cents, currency,
    discrepancy_reason, resolution_action, version, created_at, updated_at
FROM reconciliation_anomalies
WHERE tenant_id = $1 AND status = $2
ORDER BY created_at DESC;
`

	FLOW_UPDATE_RECONCILIATION_ANOMALY_STATUS = `
UPDATE reconciliation_anomalies
SET 
    status = $1,
    resolution_action = $2,
    version = version + 1,
    updated_at = NOW()
WHERE tenant_id = $3 AND id = $4 AND version = $5;
`
)

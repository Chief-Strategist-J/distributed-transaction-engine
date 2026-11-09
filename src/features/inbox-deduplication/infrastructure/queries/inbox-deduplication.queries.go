package queries

const FLOW_INSERT_INBOX_RECORD = `
INSERT INTO inbox_messages (
    id, public_id, tenant_id, source, external_id, correlation_id,
    handler_name, handler_generation, content_hash, status, attempt_count,
    producer_event_time, payload, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (tenant_id, handler_name, handler_generation, external_id) DO NOTHING;
`

const FLOW_GET_INBOX_RECORD = `
SELECT id, public_id, tenant_id, source, external_id, correlation_id,
       handler_name, handler_generation, content_hash, status, attempt_count,
       producer_event_time, payload, version, created_at, updated_at
FROM inbox_messages
WHERE tenant_id = $1 AND handler_name = $2 AND handler_generation = $3 AND external_id = $4;
`

const FLOW_MARK_QUARANTINED = `
UPDATE inbox_messages
SET status = 'QUARANTINED', updated_at = NOW()
WHERE tenant_id = $1 AND id = $2;
`


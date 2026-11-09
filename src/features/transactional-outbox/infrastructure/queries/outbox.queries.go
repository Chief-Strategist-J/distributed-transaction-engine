package queries

const (
	FLOW_INSERT_OUTBOX_EVENT = `
INSERT INTO outbox_events (
    id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
    aggregate_sequence, destination_topic, partition_key, event_type,
    payload, headers, status, attempt_count, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10,
    $11::jsonb, $12::jsonb, 'PENDING', 0, 1, NOW(), NOW()
);`

	FLOW_CLAIM_PENDING_OUTBOX_EVENTS = `
UPDATE outbox_events
SET status = 'IN_FLIGHT',
    lease_owner = $1,
    lease_expires_at = $2,
    attempt_count = attempt_count + 1,
    updated_at = NOW()
WHERE id IN (
    SELECT id
    FROM outbox_events
    WHERE tenant_id = $3
      AND destination_topic = $4
      AND status = 'PENDING'
    ORDER BY aggregate_sequence ASC
    LIMIT $5
    FOR UPDATE SKIP LOCKED
)
RETURNING id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
          aggregate_sequence, destination_topic, partition_key, event_type,
          payload, headers, status, lease_owner, lease_expires_at, attempt_count,
          last_error, version, created_at, updated_at, published_at;`

	FLOW_CONFIRM_OUTBOX_PUBLISHED = `
UPDATE outbox_events
SET status = 'PUBLISHED',
    published_at = NOW(),
    updated_at = NOW()
WHERE tenant_id = $1
  AND id = $2
  AND lease_owner = $3
  AND status = 'IN_FLIGHT';`

	FLOW_RELEASE_IN_FLIGHT_TO_PENDING = `
UPDATE outbox_events
SET status = 'PENDING',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = $1,
    updated_at = NOW()
WHERE tenant_id = $2
  AND id = $3
  AND status = 'IN_FLIGHT';`

	FLOW_PARK_OUTBOX_EVENT = `
UPDATE outbox_events
SET status = 'PARKED',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = $1,
    updated_at = NOW()
WHERE tenant_id = $2
  AND id = $3
  AND lease_owner = $4
  AND status = 'IN_FLIGHT';`

	FLOW_GET_OUTBOX_EVENT = `
SELECT id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
       aggregate_sequence, destination_topic, partition_key, event_type,
       payload, headers, status, lease_owner, lease_expires_at, attempt_count,
       last_error, version, created_at, updated_at, published_at
FROM outbox_events
WHERE tenant_id = $1 AND id = $2;`
)

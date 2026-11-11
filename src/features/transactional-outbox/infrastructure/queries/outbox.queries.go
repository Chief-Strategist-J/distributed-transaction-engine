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
WITH candidates AS (
    SELECT DISTINCT ON (e.aggregate_id) e.id, e.aggregate_sequence
    FROM outbox_events e
    WHERE e.tenant_id = $3
      AND e.destination_topic = $4
      AND ((e.status = 'PENDING' AND (e.next_attempt_at IS NULL OR e.next_attempt_at <= NOW()))
           OR (e.status = 'IN_FLIGHT' AND e.lease_expires_at < NOW()))
      AND NOT EXISTS (
          SELECT 1 FROM outbox_events earlier
          WHERE earlier.tenant_id = e.tenant_id
            AND earlier.aggregate_id = e.aggregate_id
            AND earlier.aggregate_sequence < e.aggregate_sequence
            AND earlier.status <> 'PUBLISHED')
    ORDER BY e.aggregate_id, e.aggregate_sequence
),
locked AS (
    SELECT o.id
    FROM outbox_events o
    JOIN candidates c ON c.id = o.id
    ORDER BY c.aggregate_sequence ASC
    LIMIT $5
    FOR UPDATE OF o SKIP LOCKED
)
UPDATE outbox_events
SET status = 'IN_FLIGHT',
    lease_owner = $1,
    lease_expires_at = $2,
    attempt_count = attempt_count + 1,
    updated_at = NOW()
WHERE id IN (SELECT id FROM locked)
RETURNING id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
          aggregate_sequence, destination_topic, partition_key, event_type,
          payload, headers, status, lease_owner, lease_expires_at, attempt_count,
          last_error, version, created_at, updated_at, published_at;`

	FLOW_LIST_OUTBOX_RELAY_TARGETS = `
SELECT tenant_id, destination_topic
FROM outbox_events
WHERE (status = 'PENDING' AND (next_attempt_at IS NULL OR next_attempt_at <= NOW()))
   OR (status = 'IN_FLIGHT' AND lease_expires_at < NOW())
GROUP BY tenant_id, destination_topic
ORDER BY MIN(created_at) ASC
LIMIT $1;`

	FLOW_CONFIRM_OUTBOX_PUBLISHED = `
UPDATE outbox_events
SET status = 'PUBLISHED',
    published_at = NOW(),
    updated_at = NOW()
WHERE tenant_id = $1
  AND id = $2
  AND lease_owner = $3
  AND status = 'IN_FLIGHT';`

	FLOW_CONFIRM_OUTBOX_PUBLISHED_BATCH = `
UPDATE outbox_events
SET status = 'PUBLISHED',
    published_at = NOW(),
    updated_at = NOW()
WHERE tenant_id = $1
  AND id = ANY($2::uuid[])
  AND lease_owner = $3
  AND status = 'IN_FLIGHT';`

	FLOW_RELEASE_IN_FLIGHT_TO_PENDING = `
UPDATE outbox_events
SET status = 'PENDING',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = $1,
    next_attempt_at = NOW() + LEAST(INTERVAL '60 seconds', INTERVAL '1 second' * POWER(2, LEAST(attempt_count, 6))) * (0.5 + random() / 2),
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

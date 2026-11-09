package queries

const (
	QueryAcquireOrTakeoverLease = `
		INSERT INTO fencing_leases (
			id, public_id, tenant_id, source, external_id, correlation_id,
			resource_id, holder_id, epoch, lease_duration_ms,
			acquired_at, expires_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, 1, $9::integer,
			NOW(), NOW() + ($9::integer * INTERVAL '1 millisecond'), NOW(), NOW()
		)
		ON CONFLICT (tenant_id, resource_id) DO UPDATE
		SET holder_id = EXCLUDED.holder_id,
		    epoch = fencing_leases.epoch + 1,
		    lease_duration_ms = EXCLUDED.lease_duration_ms,
		    acquired_at = NOW(),
		    expires_at = NOW() + (EXCLUDED.lease_duration_ms * INTERVAL '1 millisecond'),
		    released_at = NULL,
		    updated_at = NOW()
		WHERE fencing_leases.expires_at < NOW() OR fencing_leases.released_at IS NOT NULL
		RETURNING id, public_id, tenant_id, source, external_id, correlation_id,
		          resource_id, holder_id, epoch, lease_duration_ms,
		          acquired_at, expires_at, released_at, created_at, updated_at;
	`

	QueryRenewLease = `
		UPDATE fencing_leases
		SET expires_at = NOW() + ($1::integer * INTERVAL '1 millisecond'),
		    updated_at = NOW()
		WHERE tenant_id = $2
		  AND resource_id = $3
		  AND holder_id = $4
		  AND epoch = $5
		  AND released_at IS NULL
		RETURNING id, public_id, tenant_id, source, external_id, correlation_id,
		          resource_id, holder_id, epoch, lease_duration_ms,
		          acquired_at, expires_at, released_at, created_at, updated_at;
	`

	QueryReleaseLease = `
		UPDATE fencing_leases
		SET released_at = NOW(),
		    updated_at = NOW()
		WHERE tenant_id = $1
		  AND resource_id = $2
		  AND holder_id = $3
		  AND epoch = $4
		  AND released_at IS NULL;
	`

	QueryGetLease = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       resource_id, holder_id, epoch, lease_duration_ms,
		       acquired_at, expires_at, released_at, created_at, updated_at
		FROM fencing_leases
		WHERE tenant_id = $1 AND resource_id = $2;
	`

	QueryFencedBarrier = `
		UPDATE saga_instances
		SET fencing_token = $1,
		    updated_at = NOW()
		WHERE tenant_id = $2
		  AND id = $3
		  AND fencing_token <= $1
		RETURNING fencing_token;
	`

	QueryGetSagaToken = `
		SELECT fencing_token
		FROM saga_instances
		WHERE tenant_id = $1 AND id = $2;
	`

	QueryRecordAudit = `
		INSERT INTO fencing_audits (
			id, public_id, tenant_id, resource_id,
			attempted_token, current_token, rejected_holder,
			violation_type, recorded_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
	`

	QueryGetAudits = `
		SELECT id, public_id, tenant_id, resource_id,
		       attempted_token, current_token, rejected_holder,
		       violation_type, recorded_at
		FROM fencing_audits
		WHERE tenant_id = $1 AND resource_id = $2
		ORDER BY recorded_at DESC
		LIMIT $3;
	`
)


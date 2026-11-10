package queries

const (
	FLOW_SAVE_DEEP_SESSION = `
		INSERT INTO deep_trace_sessions (
			id, public_id, tenant_id, source, external_id, correlation_id,
			target_type, target_value, sampling_rate, max_spans, expires_at, status, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		)`

	FLOW_GET_ACTIVE_DEEP_SESSION = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       target_type, target_value, sampling_rate, max_spans, expires_at, status, created_at
		FROM deep_trace_sessions
		WHERE tenant_id = $1 AND target_type = $2 AND target_value = $3
		  AND status = 'ACTIVE' AND expires_at > NOW()
		LIMIT 1`

	FLOW_UPDATE_DEEP_SESSION_STATUS = `
		UPDATE deep_trace_sessions
		SET status = $1
		WHERE tenant_id = $2 AND id = $3`

	FLOW_SAVE_SAMPLING_RULE = `
		INSERT INTO trace_sampling_rules (
			id, public_id, tenant_id, source, external_id, correlation_id,
			rule_name, match_criteria, sample_rate, is_vip, enabled, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)`

	FLOW_GET_SAMPLING_RULE = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       rule_name, match_criteria, sample_rate, is_vip, enabled, created_at
		FROM trace_sampling_rules
		WHERE tenant_id = $1 AND enabled = true
		ORDER BY is_vip DESC, created_at DESC
		LIMIT 1`

	FLOW_SAVE_SPAN = `
		INSERT INTO trace_spans_buffer (
			id, public_id, tenant_id, source, external_id, correlation_id,
			trace_id, span_id, parent_span_id, span_name, span_kind,
			start_time, end_time, duration_ns, status_code, attributes, events, links, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
		)`

	FLOW_GET_SPANS_BY_TRACE = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       trace_id, span_id, parent_span_id, span_name, span_kind,
		       start_time, end_time, duration_ns, status_code, attributes, events, links, created_at
		FROM trace_spans_buffer
		WHERE tenant_id = $1 AND trace_id = $2
		ORDER BY start_time ASC`

	FLOW_COUNT_SPANS_BY_TRACE = `
		SELECT COUNT(1)
		FROM trace_spans_buffer
		WHERE tenant_id = $1 AND trace_id = $2`

	FLOW_SAVE_ASYNC_LINK = `
		INSERT INTO trace_links_registry (
			id, public_id, tenant_id, source, external_id, correlation_id,
			saga_id, step_name, traceparent, tracestate, link_type, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)`

	FLOW_GET_ASYNC_LINK = `
		SELECT id, public_id, tenant_id, source, external_id, correlation_id,
		       saga_id, step_name, traceparent, tracestate, link_type, created_at
		FROM trace_links_registry
		WHERE tenant_id = $1 AND saga_id = $2 AND step_name = $3 AND link_type = $4
		LIMIT 1`
)


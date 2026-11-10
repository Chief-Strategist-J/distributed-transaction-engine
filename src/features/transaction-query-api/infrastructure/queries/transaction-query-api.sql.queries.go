package queries

type ColumnKind int

const (
	KindText ColumnKind = iota
	KindNullText
	KindTime
	KindNullTime
	KindInt64
	KindBool
	KindTextArray
	KindNullInt64
)

type ColumnSpec struct {
	Expression string
	Kind       ColumnKind
	Operand    ColumnKind
}

var ViewColumns = map[string]ColumnSpec{
	"id":                  {Expression: "public_id", Kind: KindText, Operand: KindText},
	"status":              {Expression: "business_status", Kind: KindText, Operand: KindText},
	"internal_status":     {Expression: "internal_status", Kind: KindText, Operand: KindText},
	"workflow_type":       {Expression: "workflow_type", Kind: KindText, Operand: KindText},
	"workflow_version":    {Expression: "workflow_version", Kind: KindText, Operand: KindText},
	"created_at":          {Expression: "created_at", Kind: KindTime, Operand: KindTime},
	"completed_at":        {Expression: "completed_at", Kind: KindNullTime, Operand: KindTime},
	"effective_at":        {Expression: "effective_at", Kind: KindTime, Operand: KindTime},
	"amount":              {Expression: "amount_minor", Kind: KindInt64, Operand: KindInt64},
	"currency":            {Expression: "currency", Kind: KindText, Operand: KindText},
	"customer_ref":        {Expression: "customer_ref", Kind: KindText, Operand: KindText},
	"customer_email":      {Expression: "customer_email_blind", Kind: KindNullText, Operand: KindText},
	"merchant_ref":        {Expression: "merchant_ref", Kind: KindNullText, Operand: KindText},
	"provider_reference":  {Expression: "provider_reference", Kind: KindNullText, Operand: KindText},
	"correlation_id":      {Expression: "correlation_id", Kind: KindText, Operand: KindText},
	"tags":                {Expression: "tags", Kind: KindTextArray, Operand: KindTextArray},
	"compensation_status": {Expression: "compensation_status", Kind: KindText, Operand: KindText},
	"needs_attention":     {Expression: "needs_attention", Kind: KindBool, Operand: KindBool},
}

var ProjectionSelectColumns = map[string]ColumnSpec{
	"id":                  ViewColumns["id"],
	"status":              ViewColumns["status"],
	"internal_status":     ViewColumns["internal_status"],
	"workflow_type":       ViewColumns["workflow_type"],
	"workflow_version":    ViewColumns["workflow_version"],
	"created_at":          ViewColumns["created_at"],
	"completed_at":        ViewColumns["completed_at"],
	"effective_at":        ViewColumns["effective_at"],
	"amount":              ViewColumns["amount"],
	"currency":            ViewColumns["currency"],
	"customer_ref":        ViewColumns["customer_ref"],
	"customer_email":      {Expression: "customer_email_mask", Kind: KindNullText, Operand: KindText},
	"merchant_ref":        ViewColumns["merchant_ref"],
	"provider_reference":  ViewColumns["provider_reference"],
	"correlation_id":      ViewColumns["correlation_id"],
	"tags":                ViewColumns["tags"],
	"compensation_status": ViewColumns["compensation_status"],
	"needs_attention":     ViewColumns["needs_attention"],
}

var StepColumns = map[string]ColumnSpec{
	"steps.name":             {Expression: "name", Kind: KindText, Operand: KindText},
	"steps.status":           {Expression: "status", Kind: KindText, Operand: KindText},
	"steps.attempts":         {Expression: "attempts", Kind: KindInt64, Operand: KindInt64},
	"steps.last_error_class": {Expression: "last_error_class", Kind: KindNullText, Operand: KindText},
	"steps.completed_at":     {Expression: "completed_at", Kind: KindNullTime, Operand: KindTime},
}

var SearchColumns = map[string]ColumnSpec{
	"id":            {Expression: "public_id", Kind: KindText, Operand: KindText},
	"status":        {Expression: "business_status", Kind: KindText, Operand: KindText},
	"workflow_type": {Expression: "workflow_type", Kind: KindText, Operand: KindText},
	"created_at":    {Expression: "created_at", Kind: KindTime, Operand: KindTime},
	"completed_at":  {Expression: "completed_at", Kind: KindNullTime, Operand: KindTime},
	"currency":      {Expression: "currency", Kind: KindText, Operand: KindText},
	"customer_ref":  {Expression: "customer_ref", Kind: KindText, Operand: KindText},
	"merchant_ref":  {Expression: "merchant_ref", Kind: KindNullText, Operand: KindText},
	"tags":          {Expression: "tags", Kind: KindTextArray, Operand: KindTextArray},
	"description":   {Expression: "description_tsv", Kind: KindText, Operand: KindText},
}

const (
	TableView   = "transaction_view"
	TableStep   = "transaction_view_step"
	TableSearch = "transaction_search_document"

	FLOW_SET_TENANT_CONTEXT = `
SELECT set_config('app.tenant_id', $1, true);
`

	FLOW_SET_STATEMENT_TIMEOUT = `
SELECT set_config('statement_timeout', $1, true);
`

	FLOW_SELECT_TQ_WATERMARK = `
SELECT store, as_of_sequence, as_of_time, rebuilding
FROM tq_watermark
WHERE store = $1;
`

	FLOW_SELECT_TENANT_ROW_ESTIMATE_SOURCE = `
SELECT count(*) FROM (SELECT 1 FROM transaction_view WHERE tenant_id = $1 LIMIT 100001) AS bounded;
`

	FLOW_SELECT_TRANSACTION_SAGA_ID_PREFIX = `
SELECT v.saga_id FROM transaction_view AS v WHERE v.tenant_id = $1 AND v.public_id = $2
`

	FLOW_ACQUIRE_PROJECTION_LOCK = `
SELECT pg_advisory_xact_lock(727001);
`

	FLOW_SELECT_NEXT_PROJECTION_SEQUENCE = `
SELECT nextval('tq_projection_seq');
`

	FLOW_UPSERT_TRANSACTION_VIEW = `
INSERT INTO transaction_view (
    tenant_id, id, public_id, saga_id, internal_status, business_status, workflow_type, workflow_version,
    created_at, effective_at, completed_at, amount_minor, currency, customer_ref, customer_email_blind,
    customer_email_mask, merchant_ref, provider_reference, correlation_id, tags, description,
    compensation_status, needs_attention, owner_ref, team_ref, is_internal_only, archived_at, projection_sequence
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28
)
ON CONFLICT ON CONSTRAINT pk_transaction_view DO UPDATE SET
    internal_status = EXCLUDED.internal_status, business_status = EXCLUDED.business_status,
    workflow_version = EXCLUDED.workflow_version, effective_at = EXCLUDED.effective_at,
    completed_at = EXCLUDED.completed_at, amount_minor = EXCLUDED.amount_minor, currency = EXCLUDED.currency,
    customer_email_blind = EXCLUDED.customer_email_blind, customer_email_mask = EXCLUDED.customer_email_mask,
    merchant_ref = EXCLUDED.merchant_ref, provider_reference = EXCLUDED.provider_reference, tags = EXCLUDED.tags,
    description = EXCLUDED.description, compensation_status = EXCLUDED.compensation_status,
    needs_attention = EXCLUDED.needs_attention, owner_ref = EXCLUDED.owner_ref, team_ref = EXCLUDED.team_ref,
    is_internal_only = EXCLUDED.is_internal_only, archived_at = EXCLUDED.archived_at,
    projection_sequence = EXCLUDED.projection_sequence, version = transaction_view.version + 1;
`

	FLOW_DELETE_TRANSACTION_VIEW_STEPS = `
DELETE FROM transaction_view_step WHERE tenant_id = $1 AND transaction_id = $2;
`

	FLOW_INSERT_TRANSACTION_VIEW_STEP = `
INSERT INTO transaction_view_step (tenant_id, transaction_id, step_no, name, status, attempts, last_error_class, completed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
`

	FLOW_UPDATE_TQ_WATERMARK = `
UPDATE tq_watermark
SET as_of_sequence = GREATEST(as_of_sequence, $2), as_of_time = now()
WHERE store = $1;
`

	FLOW_UPSERT_TRANSACTION_SEARCH_DOCUMENT = `
INSERT INTO transaction_search_document (
    tenant_id, public_id, business_status, workflow_type, created_at, completed_at, currency, customer_ref,
    merchant_ref, tags, description, description_tsv, owner_ref, team_ref, is_internal_only, archived_at, index_sequence
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, to_tsvector('simple', coalesce($11, '')), $12, $13, $14, $15, $16
)
ON CONFLICT ON CONSTRAINT pk_transaction_search_document DO UPDATE SET
    business_status = EXCLUDED.business_status, completed_at = EXCLUDED.completed_at, merchant_ref = EXCLUDED.merchant_ref,
    tags = EXCLUDED.tags, description = EXCLUDED.description, description_tsv = EXCLUDED.description_tsv,
    owner_ref = EXCLUDED.owner_ref, team_ref = EXCLUDED.team_ref, is_internal_only = EXCLUDED.is_internal_only,
    archived_at = EXCLUDED.archived_at, index_sequence = EXCLUDED.index_sequence;
`

	FLOW_SELECT_TIMELINE_EVENTS_PREFIX = `
SELECT step_id, execution_state, attempt_count, error_message, idempotency_key, started_at, completed_at, created_at
FROM saga_step_executions
WHERE tenant_id = $1 AND saga_id = $2
`

	FLOW_INSERT_TQ_AUDIT_RECORD = `
INSERT INTO tq_audit_log (
    id, public_id, tenant_id, principal_id, audience, action, normalized_ast, result_count, purpose,
    classes_touched, impersonated, correlation_id, occurred_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
`

	FLOW_SELECT_TQ_RATE_BUCKET = `
SELECT tokens, extract(epoch FROM (now() - refilled_at))
FROM tq_rate_bucket
WHERE tenant_id = $1 AND bucket_key = $2;
`

	FLOW_UPDATE_TQ_RATE_BUCKET_CONSUME = `
INSERT INTO tq_rate_bucket (tenant_id, bucket_key, tokens, refilled_at)
VALUES ($1, $2, $3::double precision - $4::double precision, now())
ON CONFLICT ON CONSTRAINT pk_tq_rate_bucket DO UPDATE
SET tokens = LEAST($3::double precision, tq_rate_bucket.tokens + extract(epoch FROM (now() - tq_rate_bucket.refilled_at)) * $5::double precision) - $4::double precision,
    refilled_at = now()
WHERE LEAST($3::double precision, tq_rate_bucket.tokens + extract(epoch FROM (now() - tq_rate_bucket.refilled_at)) * $5::double precision) >= $4::double precision
RETURNING tokens;
`

	FLOW_INSERT_TQ_SAVED_QUERY = `
INSERT INTO tq_saved_query (
    id, public_id, tenant_id, owner_id, visibility, team_ref, name, tql_version, registry_version,
    query_json, status, broken_reason, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);
`

	tqSavedQueryColumns = `id, public_id, tenant_id, owner_id, visibility, team_ref, name, tql_version, registry_version,
    query_json, status, broken_reason, version, created_at, updated_at`

	FLOW_SELECT_TQ_SAVED_QUERY_BY_PUBLIC_ID = `
SELECT ` + tqSavedQueryColumns + `
FROM tq_saved_query
WHERE tenant_id = $1 AND public_id = $2;
`

	FLOW_SELECT_TQ_SAVED_QUERIES_BY_TENANT = `
SELECT ` + tqSavedQueryColumns + `
FROM tq_saved_query
WHERE tenant_id = $1
ORDER BY created_at, id
LIMIT $2;
`

	FLOW_UPDATE_TQ_SAVED_QUERY_STATUS = `
UPDATE tq_saved_query
SET status = $3, broken_reason = $4, registry_version = $5, version = version + 1
WHERE tenant_id = $1 AND id = $2 AND version = $6;
`

	FLOW_INSERT_TQ_EXPORT_JOB = `
INSERT INTO tq_export_job (
    id, public_id, tenant_id, principal_id, status, query_json, principal_json, fields, tql_version,
    as_of_sequence, row_count, failure_code, correlation_id, version, created_at, completed_at, expires_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);
`

	tqExportJobColumns = `id, public_id, tenant_id, principal_id, status, query_json, principal_json, fields, tql_version,
    as_of_sequence, row_count, failure_code, correlation_id, version, created_at, completed_at, expires_at`

	FLOW_SELECT_TQ_EXPORT_JOB_BY_PUBLIC_ID = `
SELECT ` + tqExportJobColumns + `
FROM tq_export_job
WHERE tenant_id = $1 AND public_id = $2;
`

	FLOW_UPDATE_TQ_EXPORT_JOB = `
UPDATE tq_export_job
SET status = $3, row_count = $4, failure_code = $5, completed_at = $6, version = version + 1
WHERE tenant_id = $1 AND id = $2 AND version = $7;
`

	FLOW_INSERT_TQ_EXPORT_CHUNK = `
INSERT INTO tq_export_chunk (tenant_id, job_id, chunk_no, ciphertext, row_count)
VALUES ($1, $2, $3, $4, $5);
`

	FLOW_SELECT_TQ_EXPORT_CHUNKS_BY_JOB = `
SELECT job_id, chunk_no, ciphertext, row_count
FROM tq_export_chunk
WHERE tenant_id = $1 AND job_id = $2
ORDER BY chunk_no;
`

	FLOW_CHECK_TRANSACTION_VIEW_STEP_ORPHANS = `
SELECT step.tenant_id
FROM transaction_view_step AS step
WHERE NOT EXISTS (SELECT 1 FROM transaction_view AS v WHERE v.tenant_id = step.tenant_id AND v.id = step.transaction_id);
`

	FLOW_CHECK_TQ_EXPORT_JOB_COMPLETED_WITHOUT_CHUNKS = `
SELECT job.public_id
FROM tq_export_job AS job
WHERE job.status = 'COMPLETED' AND job.row_count > 0
  AND NOT EXISTS (SELECT 1 FROM tq_export_chunk AS chunk WHERE chunk.tenant_id = job.tenant_id AND chunk.job_id = job.id);
`
)

-- migration:      0023
-- description:    create the transaction read model, search index, step projection, watermarks, query audit log, rate buckets, saved queries and export jobs with forced row level security
-- author:         lead-architect
-- date:           2026-11-10T14:00:00Z
-- depends_on:     0022_create_tenant_isolation_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         give the transaction query language an authoritative indexed read model, a derived search store, tenant-scoped governance tables and append-only audit
-- data_check:     SELECT count(*) FROM transaction_view WHERE (business_status IN ('COMPLETED','FAILED','COMPENSATED','CANCELLED')) <> (completed_at IS NOT NULL); (must be 0 before enforcement)
-- lock_note:      new tables only; indexes are created on empty tables; no ACCESS EXCLUSIVE lock on existing tables

CREATE SEQUENCE IF NOT EXISTS tq_projection_seq AS bigint START WITH 1 MINVALUE 1;

CREATE TABLE IF NOT EXISTS tq_watermark (
    store VARCHAR(16) NOT NULL,
    as_of_sequence BIGINT NOT NULL DEFAULT 0,
    as_of_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rebuilding BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_tq_watermark PRIMARY KEY (store),
    CONSTRAINT tq_watermark_store_ck CHECK (store IN ('PRIMARY', 'SEARCH')),
    CONSTRAINT tq_watermark_as_of_sequence_ck CHECK (as_of_sequence >= 0)
);

INSERT INTO tq_watermark (store) VALUES ('PRIMARY'), ('SEARCH') ON CONFLICT (store) DO NOTHING;

CREATE TABLE IF NOT EXISTS transaction_view (
    tenant_id VARCHAR(64) NOT NULL,
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    saga_id UUID NOT NULL,
    internal_status VARCHAR(32) NOT NULL,
    business_status VARCHAR(16) NOT NULL,
    workflow_type VARCHAR(128) NOT NULL,
    workflow_version VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    customer_ref VARCHAR(128) NOT NULL,
    customer_email_blind CHAR(64),
    customer_email_mask VARCHAR(256),
    merchant_ref VARCHAR(128),
    provider_reference VARCHAR(128),
    correlation_id VARCHAR(64) NOT NULL,
    tags TEXT[] NOT NULL DEFAULT '{}',
    description VARCHAR(4000),
    compensation_status VARCHAR(16) NOT NULL DEFAULT 'NONE',
    needs_attention BOOLEAN NOT NULL DEFAULT FALSE,
    owner_ref VARCHAR(128) NOT NULL,
    team_ref VARCHAR(64),
    is_internal_only BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at TIMESTAMPTZ,
    projection_sequence BIGINT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_transaction_view PRIMARY KEY (tenant_id, id),
    CONSTRAINT transaction_view_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT transaction_view_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT transaction_view_public_id_ck CHECK (public_id ~ '^txn_[0-9a-zA-Z]{26}$'),
    CONSTRAINT transaction_view_internal_status_ck CHECK (internal_status IN ('CREATED','RUNNING','ROLLING_FORWARD','STUCK_FORWARD','COMPENSATING','COMPENSATION_STUCK','COMPLETED','FAILED','FAILED_COMPENSATED','CANCELLED')),
    CONSTRAINT transaction_view_business_status_ck CHECK (business_status IN ('PENDING','PROCESSING','COMPLETED','FAILED','COMPENSATING','COMPENSATED','CANCELLED')),
    CONSTRAINT transaction_view_compensation_status_ck CHECK (compensation_status IN ('NONE','IN_PROGRESS','COMPLETED','FAILED')),
    CONSTRAINT transaction_view_completed_at_ck CHECK ((business_status IN ('COMPLETED','FAILED','COMPENSATED','CANCELLED')) = (completed_at IS NOT NULL)),
    CONSTRAINT transaction_view_amount_minor_ck CHECK (amount_minor >= 0),
    CONSTRAINT transaction_view_currency_ck CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT transaction_view_workflow_type_ck CHECK (workflow_type = btrim(workflow_type) AND workflow_type <> ''),
    CONSTRAINT transaction_view_workflow_version_ck CHECK (workflow_version = btrim(workflow_version) AND workflow_version <> ''),
    CONSTRAINT transaction_view_customer_ref_ck CHECK (customer_ref = btrim(customer_ref) AND customer_ref <> ''),
    CONSTRAINT transaction_view_customer_email_blind_ck CHECK (customer_email_blind IS NULL OR customer_email_blind ~ '^[0-9a-f]{64}$'),
    CONSTRAINT transaction_view_merchant_ref_ck CHECK (merchant_ref IS NULL OR (merchant_ref = btrim(merchant_ref) AND merchant_ref <> '')),
    CONSTRAINT transaction_view_provider_reference_ck CHECK (provider_reference IS NULL OR (provider_reference = btrim(provider_reference) AND provider_reference <> '')),
    CONSTRAINT transaction_view_correlation_id_ck CHECK (correlation_id = btrim(correlation_id) AND correlation_id <> ''),
    CONSTRAINT transaction_view_tags_ck CHECK (cardinality(tags) <= 50),
    CONSTRAINT transaction_view_owner_ref_ck CHECK (owner_ref = btrim(owner_ref) AND owner_ref <> ''),
    CONSTRAINT transaction_view_projection_sequence_ck CHECK (projection_sequence >= 1),
    CONSTRAINT transaction_view_version_ck CHECK (version >= 1)
);

CREATE INDEX IF NOT EXISTS idx_transaction_view_created ON transaction_view (tenant_id, created_at, public_id);
CREATE INDEX IF NOT EXISTS idx_transaction_view_completed ON transaction_view (tenant_id, completed_at, public_id);
CREATE INDEX IF NOT EXISTS idx_transaction_view_effective ON transaction_view (tenant_id, effective_at, public_id);
CREATE INDEX IF NOT EXISTS idx_transaction_view_status_created ON transaction_view (tenant_id, business_status, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_view_internal_status ON transaction_view (tenant_id, internal_status, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_view_workflow_type ON transaction_view (tenant_id, workflow_type, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_view_workflow_version ON transaction_view (tenant_id, workflow_version);
CREATE INDEX IF NOT EXISTS idx_transaction_view_currency_amount ON transaction_view (tenant_id, currency, amount_minor);
CREATE INDEX IF NOT EXISTS idx_transaction_view_customer_ref ON transaction_view (tenant_id, customer_ref);
CREATE INDEX IF NOT EXISTS idx_transaction_view_customer_email ON transaction_view (tenant_id, customer_email_blind) WHERE customer_email_blind IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transaction_view_merchant_ref ON transaction_view (tenant_id, merchant_ref text_pattern_ops) WHERE merchant_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transaction_view_provider_reference ON transaction_view (tenant_id, provider_reference) WHERE provider_reference IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transaction_view_correlation ON transaction_view (tenant_id, correlation_id);
CREATE INDEX IF NOT EXISTS idx_transaction_view_compensation ON transaction_view (tenant_id, compensation_status);
CREATE INDEX IF NOT EXISTS idx_transaction_view_attention ON transaction_view (tenant_id, created_at) WHERE needs_attention;
CREATE INDEX IF NOT EXISTS idx_transaction_view_tags ON transaction_view USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_transaction_view_owner ON transaction_view (tenant_id, owner_ref, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_view_team ON transaction_view (tenant_id, team_ref, created_at) WHERE team_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transaction_view_sequence ON transaction_view (projection_sequence);

CREATE TABLE IF NOT EXISTS transaction_view_step (
    tenant_id VARCHAR(64) NOT NULL,
    transaction_id UUID NOT NULL,
    step_no INTEGER NOT NULL,
    name VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error_class VARCHAR(64),
    completed_at TIMESTAMPTZ,
    CONSTRAINT pk_transaction_view_step PRIMARY KEY (tenant_id, transaction_id, step_no),
    CONSTRAINT transaction_view_step_transaction_fk FOREIGN KEY (tenant_id, transaction_id) REFERENCES transaction_view (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT transaction_view_step_step_no_ck CHECK (step_no >= 1),
    CONSTRAINT transaction_view_step_name_ck CHECK (name = btrim(name) AND name <> ''),
    CONSTRAINT transaction_view_step_status_ck CHECK (status IN ('NOT_STARTED','PENDING','DISPATCHED','RETRY_WAIT','SUCCEEDED','FAILED','UNKNOWN','COMPENSATING','COMPENSATED','SKIPPED')),
    CONSTRAINT transaction_view_step_attempts_ck CHECK (attempts >= 0),
    CONSTRAINT transaction_view_step_last_error_class_ck CHECK (last_error_class IS NULL OR (last_error_class = btrim(last_error_class) AND last_error_class <> ''))
);

CREATE INDEX IF NOT EXISTS idx_transaction_view_step_name ON transaction_view_step (tenant_id, name, status);
CREATE INDEX IF NOT EXISTS idx_transaction_view_step_status ON transaction_view_step (tenant_id, status, transaction_id);
CREATE INDEX IF NOT EXISTS idx_transaction_view_step_error ON transaction_view_step (tenant_id, last_error_class) WHERE last_error_class IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transaction_view_step_completed ON transaction_view_step (tenant_id, completed_at) WHERE completed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transaction_view_step_attempts ON transaction_view_step (tenant_id, attempts);

CREATE TABLE IF NOT EXISTS transaction_search_document (
    tenant_id VARCHAR(64) NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    business_status VARCHAR(16) NOT NULL,
    workflow_type VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    currency CHAR(3) NOT NULL,
    customer_ref VARCHAR(128) NOT NULL,
    merchant_ref VARCHAR(128),
    tags TEXT[] NOT NULL DEFAULT '{}',
    description VARCHAR(4000),
    description_tsv TSVECTOR NOT NULL,
    owner_ref VARCHAR(128) NOT NULL,
    team_ref VARCHAR(64),
    is_internal_only BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at TIMESTAMPTZ,
    index_sequence BIGINT NOT NULL,
    CONSTRAINT pk_transaction_search_document PRIMARY KEY (tenant_id, public_id),
    CONSTRAINT transaction_search_document_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT transaction_search_document_public_id_ck CHECK (public_id ~ '^txn_[0-9a-zA-Z]{26}$'),
    CONSTRAINT transaction_search_document_business_status_ck CHECK (business_status IN ('PENDING','PROCESSING','COMPLETED','FAILED','COMPENSATING','COMPENSATED','CANCELLED')),
    CONSTRAINT transaction_search_document_currency_ck CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT transaction_search_document_tags_ck CHECK (cardinality(tags) <= 50),
    CONSTRAINT transaction_search_document_index_sequence_ck CHECK (index_sequence >= 1)
);

CREATE INDEX IF NOT EXISTS idx_transaction_search_document_created ON transaction_search_document (tenant_id, created_at, public_id);
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_completed ON transaction_search_document (tenant_id, completed_at, public_id);
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_status ON transaction_search_document (tenant_id, business_status, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_workflow ON transaction_search_document (tenant_id, workflow_type, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_customer ON transaction_search_document (tenant_id, customer_ref);
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_merchant ON transaction_search_document (tenant_id, merchant_ref text_pattern_ops) WHERE merchant_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_currency ON transaction_search_document (tenant_id, currency, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_tags ON transaction_search_document USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_transaction_search_document_tsv ON transaction_search_document USING GIN (description_tsv);

CREATE TABLE IF NOT EXISTS tq_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    audience VARCHAR(24) NOT NULL,
    action VARCHAR(24) NOT NULL,
    normalized_ast JSONB NOT NULL,
    result_count BIGINT,
    purpose VARCHAR(256),
    classes_touched TEXT[] NOT NULL DEFAULT '{}',
    impersonated BOOLEAN NOT NULL DEFAULT FALSE,
    correlation_id UUID NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_tq_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT tq_audit_log_public_id_uk UNIQUE (public_id),
    CONSTRAINT tq_audit_log_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT tq_audit_log_public_id_ck CHECK (public_id ~ '^tqa_[0-9a-zA-Z]{26}$'),
    CONSTRAINT tq_audit_log_principal_id_ck CHECK (principal_id = btrim(principal_id) AND principal_id <> ''),
    CONSTRAINT tq_audit_log_audience_ck CHECK (audience IN ('TENANT_USER','TENANT_API_CLIENT','OPERATOR','FINANCE_AUDITOR','INTERNAL_SERVICE')),
    CONSTRAINT tq_audit_log_action_ck CHECK (action IN ('SEARCH','AGGREGATE','GET','TIMELINE','EXPORT_CREATE','EXPORT_DOWNLOAD','SAVED_QUERY_RUN')),
    CONSTRAINT tq_audit_log_normalized_ast_ck CHECK (jsonb_typeof(normalized_ast) = 'object'),
    CONSTRAINT tq_audit_log_result_count_ck CHECK (result_count IS NULL OR result_count >= 0),
    CONSTRAINT tq_audit_log_purpose_ck CHECK (purpose IS NULL OR (purpose = btrim(purpose) AND purpose <> ''))
);

CREATE INDEX IF NOT EXISTS idx_tq_audit_log_principal ON tq_audit_log (tenant_id, principal_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_tq_audit_log_action ON tq_audit_log (tenant_id, action, occurred_at DESC);

CREATE TABLE IF NOT EXISTS tq_rate_bucket (
    tenant_id VARCHAR(64) NOT NULL,
    bucket_key VARCHAR(160) NOT NULL,
    tokens DOUBLE PRECISION NOT NULL,
    refilled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_tq_rate_bucket PRIMARY KEY (tenant_id, bucket_key),
    CONSTRAINT tq_rate_bucket_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT tq_rate_bucket_bucket_key_ck CHECK (bucket_key = btrim(bucket_key) AND bucket_key <> ''),
    CONSTRAINT tq_rate_bucket_tokens_ck CHECK (tokens >= 0)
);

CREATE TABLE IF NOT EXISTS tq_saved_query (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    visibility VARCHAR(8) NOT NULL,
    team_ref VARCHAR(64),
    name VARCHAR(128) NOT NULL,
    tql_version VARCHAR(8) NOT NULL,
    registry_version VARCHAR(16) NOT NULL,
    query_json JSONB NOT NULL,
    status VARCHAR(8) NOT NULL DEFAULT 'VALID',
    broken_reason VARCHAR(256),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_tq_saved_query PRIMARY KEY (tenant_id, id),
    CONSTRAINT tq_saved_query_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT tq_saved_query_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT tq_saved_query_public_id_ck CHECK (public_id ~ '^sqr_[0-9a-zA-Z]{26}$'),
    CONSTRAINT tq_saved_query_owner_id_ck CHECK (owner_id = btrim(owner_id) AND owner_id <> ''),
    CONSTRAINT tq_saved_query_visibility_ck CHECK (visibility IN ('PRIVATE','TEAM')),
    CONSTRAINT tq_saved_query_team_ref_ck CHECK ((visibility = 'TEAM') = (team_ref IS NOT NULL)),
    CONSTRAINT tq_saved_query_name_ck CHECK (name = btrim(name) AND name <> ''),
    CONSTRAINT tq_saved_query_query_json_ck CHECK (jsonb_typeof(query_json) = 'object'),
    CONSTRAINT tq_saved_query_status_ck CHECK (status IN ('VALID','BROKEN')),
    CONSTRAINT tq_saved_query_broken_reason_ck CHECK ((status = 'BROKEN') = (broken_reason IS NOT NULL)),
    CONSTRAINT tq_saved_query_version_ck CHECK (version >= 1)
);

CREATE INDEX IF NOT EXISTS idx_tq_saved_query_owner ON tq_saved_query (tenant_id, owner_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tq_saved_query_team ON tq_saved_query (tenant_id, team_ref) WHERE team_ref IS NOT NULL;

CREATE TABLE IF NOT EXISTS tq_export_status (
    code VARCHAR(16) NOT NULL,
    CONSTRAINT pk_tq_export_status PRIMARY KEY (code)
);

INSERT INTO tq_export_status (code) VALUES ('QUEUED'), ('RUNNING'), ('COMPLETED'), ('FAILED'), ('EXPIRED'), ('CANCELLED')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS tq_export_status_transition (
    from_status VARCHAR(16) NOT NULL,
    to_status VARCHAR(16) NOT NULL,
    CONSTRAINT pk_tq_export_status_transition PRIMARY KEY (from_status, to_status),
    CONSTRAINT tq_export_status_transition_from_fk FOREIGN KEY (from_status) REFERENCES tq_export_status (code) ON DELETE RESTRICT,
    CONSTRAINT tq_export_status_transition_to_fk FOREIGN KEY (to_status) REFERENCES tq_export_status (code) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_tq_export_status_transition_to ON tq_export_status_transition (to_status);

INSERT INTO tq_export_status_transition (from_status, to_status) VALUES
    ('QUEUED', 'RUNNING'), ('QUEUED', 'CANCELLED'), ('QUEUED', 'EXPIRED'),
    ('RUNNING', 'COMPLETED'), ('RUNNING', 'FAILED'), ('RUNNING', 'CANCELLED'),
    ('COMPLETED', 'EXPIRED'), ('FAILED', 'EXPIRED')
ON CONFLICT (from_status, to_status) DO NOTHING;

CREATE TABLE IF NOT EXISTS tq_export_job (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'QUEUED',
    query_json JSONB NOT NULL,
    principal_json JSONB NOT NULL,
    fields TEXT[] NOT NULL,
    tql_version VARCHAR(8) NOT NULL,
    as_of_sequence BIGINT NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    failure_code VARCHAR(64),
    correlation_id UUID NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_tq_export_job PRIMARY KEY (tenant_id, id),
    CONSTRAINT tq_export_job_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT tq_export_job_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT tq_export_job_status_fk FOREIGN KEY (status) REFERENCES tq_export_status (code) ON DELETE RESTRICT,
    CONSTRAINT tq_export_job_public_id_ck CHECK (public_id ~ '^exp_[0-9a-zA-Z]{26}$'),
    CONSTRAINT tq_export_job_principal_id_ck CHECK (principal_id = btrim(principal_id) AND principal_id <> ''),
    CONSTRAINT tq_export_job_query_json_ck CHECK (jsonb_typeof(query_json) = 'object'),
    CONSTRAINT tq_export_job_principal_json_ck CHECK (jsonb_typeof(principal_json) = 'object'),
    CONSTRAINT tq_export_job_fields_ck CHECK (cardinality(fields) BETWEEN 1 AND 40),
    CONSTRAINT tq_export_job_as_of_sequence_ck CHECK (as_of_sequence >= 0),
    CONSTRAINT tq_export_job_row_count_ck CHECK (row_count >= 0 AND row_count <= 100000),
    CONSTRAINT tq_export_job_failure_code_ck CHECK ((status = 'FAILED') = (failure_code IS NOT NULL)),
    CONSTRAINT tq_export_job_completed_at_ck CHECK ((status IN ('COMPLETED','FAILED')) = (completed_at IS NOT NULL) OR status IN ('EXPIRED','CANCELLED')),
    CONSTRAINT tq_export_job_version_ck CHECK (version >= 1)
);

CREATE INDEX IF NOT EXISTS idx_tq_export_job_principal ON tq_export_job (tenant_id, principal_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tq_export_job_status ON tq_export_job (tenant_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_tq_export_job_status_fk ON tq_export_job (status);

CREATE TABLE IF NOT EXISTS tq_export_chunk (
    tenant_id VARCHAR(64) NOT NULL,
    job_id UUID NOT NULL,
    chunk_no INTEGER NOT NULL,
    ciphertext BYTEA NOT NULL,
    row_count INTEGER NOT NULL,
    CONSTRAINT pk_tq_export_chunk PRIMARY KEY (tenant_id, job_id, chunk_no),
    CONSTRAINT tq_export_chunk_job_fk FOREIGN KEY (tenant_id, job_id) REFERENCES tq_export_job (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT tq_export_chunk_chunk_no_ck CHECK (chunk_no >= 1),
    CONSTRAINT tq_export_chunk_row_count_ck CHECK (row_count >= 0)
);

CREATE OR REPLACE FUNCTION tq_guard_export_job_transition() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status AND NOT EXISTS (
        SELECT 1 FROM tq_export_status_transition WHERE from_status = OLD.status AND to_status = NEW.status
    ) THEN
        RAISE EXCEPTION 'export status transition % -> % is not allowed', OLD.status, NEW.status
            USING ERRCODE = '23514', CONSTRAINT = 'tq_export_job_status_transition_ck';
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS tq_export_job_guard_transition_tr ON tq_export_job;
CREATE TRIGGER tq_export_job_guard_transition_tr BEFORE UPDATE ON tq_export_job
    FOR EACH ROW EXECUTE FUNCTION tq_guard_export_job_transition();

DROP TRIGGER IF EXISTS tq_export_job_guard_immutable_tr ON tq_export_job;
CREATE TRIGGER tq_export_job_guard_immutable_tr BEFORE UPDATE ON tq_export_job
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_immutable('id', 'public_id', 'tenant_id', 'principal_id', 'query_json', 'principal_json', 'fields', 'tql_version', 'as_of_sequence', 'correlation_id', 'created_at');

DROP TRIGGER IF EXISTS tq_saved_query_guard_immutable_tr ON tq_saved_query;
CREATE TRIGGER tq_saved_query_guard_immutable_tr BEFORE UPDATE ON tq_saved_query
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_immutable('id', 'public_id', 'tenant_id', 'owner_id', 'visibility', 'team_ref', 'name', 'query_json', 'tql_version', 'created_at');

DROP TRIGGER IF EXISTS transaction_view_guard_immutable_tr ON transaction_view;
CREATE TRIGGER transaction_view_guard_immutable_tr BEFORE UPDATE ON transaction_view
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_immutable('id', 'public_id', 'tenant_id', 'saga_id', 'created_at', 'workflow_type', 'correlation_id', 'customer_ref');

DROP TRIGGER IF EXISTS transaction_view_touch_updated_at_tr ON transaction_view;
CREATE TRIGGER transaction_view_touch_updated_at_tr BEFORE UPDATE ON transaction_view
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_touch_updated_at();

DROP TRIGGER IF EXISTS tq_audit_log_reject_mutation_tr ON tq_audit_log;
CREATE TRIGGER tq_audit_log_reject_mutation_tr BEFORE UPDATE OR DELETE ON tq_audit_log
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_reject_mutation();

REVOKE ALL ON tq_watermark, transaction_view, transaction_view_step, transaction_search_document, tq_audit_log,
    tq_rate_bucket, tq_saved_query, tq_export_status, tq_export_status_transition, tq_export_job, tq_export_chunk FROM PUBLIC;

GRANT SELECT ON tq_watermark, tq_export_status, tq_export_status_transition TO dtx_app, dtx_platform;
GRANT UPDATE ON tq_watermark TO dtx_platform;
GRANT USAGE, SELECT ON SEQUENCE tq_projection_seq TO dtx_platform;
GRANT SELECT ON transaction_view, transaction_view_step, transaction_search_document TO dtx_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON transaction_view, transaction_view_step, transaction_search_document TO dtx_platform;
GRANT INSERT ON tq_audit_log TO dtx_app;
GRANT SELECT, INSERT ON tq_audit_log TO dtx_platform;
GRANT SELECT, INSERT, UPDATE ON tq_rate_bucket TO dtx_app, dtx_platform;
GRANT SELECT, INSERT, UPDATE ON tq_saved_query, tq_export_job TO dtx_app, dtx_platform;
GRANT SELECT, INSERT ON tq_export_chunk TO dtx_app, dtx_platform;

ALTER TABLE transaction_view ENABLE ROW LEVEL SECURITY;
ALTER TABLE transaction_view FORCE ROW LEVEL SECURITY;
ALTER TABLE transaction_view_step ENABLE ROW LEVEL SECURITY;
ALTER TABLE transaction_view_step FORCE ROW LEVEL SECURITY;
ALTER TABLE transaction_search_document ENABLE ROW LEVEL SECURITY;
ALTER TABLE transaction_search_document FORCE ROW LEVEL SECURITY;
ALTER TABLE tq_audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE tq_audit_log FORCE ROW LEVEL SECURITY;
ALTER TABLE tq_rate_bucket ENABLE ROW LEVEL SECURITY;
ALTER TABLE tq_rate_bucket FORCE ROW LEVEL SECURITY;
ALTER TABLE tq_saved_query ENABLE ROW LEVEL SECURITY;
ALTER TABLE tq_saved_query FORCE ROW LEVEL SECURITY;
ALTER TABLE tq_export_job ENABLE ROW LEVEL SECURITY;
ALTER TABLE tq_export_job FORCE ROW LEVEL SECURITY;
ALTER TABLE tq_export_chunk ENABLE ROW LEVEL SECURITY;
ALTER TABLE tq_export_chunk FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS transaction_view_app_read ON transaction_view;
CREATE POLICY transaction_view_app_read ON transaction_view FOR SELECT TO dtx_app USING (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS transaction_view_platform_all ON transaction_view;
CREATE POLICY transaction_view_platform_all ON transaction_view FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS transaction_view_step_app_read ON transaction_view_step;
CREATE POLICY transaction_view_step_app_read ON transaction_view_step FOR SELECT TO dtx_app USING (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS transaction_view_step_platform_all ON transaction_view_step;
CREATE POLICY transaction_view_step_platform_all ON transaction_view_step FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS transaction_search_document_app_read ON transaction_search_document;
CREATE POLICY transaction_search_document_app_read ON transaction_search_document FOR SELECT TO dtx_app USING (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS transaction_search_document_platform_all ON transaction_search_document;
CREATE POLICY transaction_search_document_platform_all ON transaction_search_document FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tq_audit_log_app_insert ON tq_audit_log;
CREATE POLICY tq_audit_log_app_insert ON tq_audit_log FOR INSERT TO dtx_app WITH CHECK (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS tq_audit_log_platform_all ON tq_audit_log;
CREATE POLICY tq_audit_log_platform_all ON tq_audit_log FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tq_rate_bucket_app_isolation ON tq_rate_bucket;
CREATE POLICY tq_rate_bucket_app_isolation ON tq_rate_bucket FOR ALL TO dtx_app USING (tenant_id = tenant_isolation_current_tenant()) WITH CHECK (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS tq_rate_bucket_platform_all ON tq_rate_bucket;
CREATE POLICY tq_rate_bucket_platform_all ON tq_rate_bucket FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tq_saved_query_app_isolation ON tq_saved_query;
CREATE POLICY tq_saved_query_app_isolation ON tq_saved_query FOR ALL TO dtx_app USING (tenant_id = tenant_isolation_current_tenant()) WITH CHECK (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS tq_saved_query_platform_all ON tq_saved_query;
CREATE POLICY tq_saved_query_platform_all ON tq_saved_query FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tq_export_job_app_isolation ON tq_export_job;
CREATE POLICY tq_export_job_app_isolation ON tq_export_job FOR ALL TO dtx_app USING (tenant_id = tenant_isolation_current_tenant()) WITH CHECK (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS tq_export_job_platform_all ON tq_export_job;
CREATE POLICY tq_export_job_platform_all ON tq_export_job FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tq_export_chunk_app_isolation ON tq_export_chunk;
CREATE POLICY tq_export_chunk_app_isolation ON tq_export_chunk FOR ALL TO dtx_app USING (tenant_id = tenant_isolation_current_tenant()) WITH CHECK (tenant_id = tenant_isolation_current_tenant());
DROP POLICY IF EXISTS tq_export_chunk_platform_all ON tq_export_chunk;
CREATE POLICY tq_export_chunk_platform_all ON tq_export_chunk FOR ALL TO dtx_platform USING (true) WITH CHECK (true);

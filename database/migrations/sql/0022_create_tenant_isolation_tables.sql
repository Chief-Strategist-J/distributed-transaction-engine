-- migration:      0022
-- description:    create tenant registry, lifecycle transitions, saga quota counters, message quarantine, security events and forced row level security
-- author:         lead-architect
-- date:           2026-11-10T12:00:00Z
-- depends_on:     0021_create_distributed_tracing_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce tenant isolation at the storage gate: tenant-scoped keys, forced RLS with USING and WITH CHECK, deny-by-default tenant context, atomic quota reservation, tombstoned tenant ids
-- data_check:     SELECT count(*) FROM tenant_registry WHERE status = 'SUSPENDED' AND suspension_reason IS NULL; (must be 0 on any populated database before enforcement)
-- lock_note:      new tables only; no ACCESS EXCLUSIVE lock on existing tables; indexes created on empty tables
-- roles:          dtx_app (tenant-scoped application path), dtx_platform (platform operations); both NOLOGIN, NOSUPERUSER, NOBYPASSRLS; cluster-level and intentionally kept by rollback

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dtx_app') THEN
        CREATE ROLE dtx_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dtx_platform') THEN
        CREATE ROLE dtx_platform NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
    END IF;
END
$roles$;

CREATE OR REPLACE FUNCTION tenant_isolation_current_tenant() RETURNS text
LANGUAGE sql STABLE SECURITY INVOKER
AS $$ SELECT NULLIF(current_setting('app.tenant_id', true), '') $$;

CREATE OR REPLACE FUNCTION tenant_isolation_guard_immutable() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
AS $$
DECLARE
    guarded_column text;
BEGIN
    FOREACH guarded_column IN ARRAY TG_ARGV LOOP
        IF to_jsonb(NEW) -> guarded_column IS DISTINCT FROM to_jsonb(OLD) -> guarded_column THEN
            RAISE EXCEPTION 'column % of % is immutable', guarded_column, TG_TABLE_NAME
                USING ERRCODE = '23514', CONSTRAINT = TG_TABLE_NAME || '_' || guarded_column || '_immutable_ck';
        END IF;
    END LOOP;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION tenant_isolation_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION tenant_isolation_reject_mutation() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME
        USING ERRCODE = '23514', CONSTRAINT = TG_TABLE_NAME || '_append_only_ck';
END
$$;

CREATE TABLE IF NOT EXISTS tenant_status (
    code VARCHAR(16) NOT NULL,
    CONSTRAINT pk_tenant_status PRIMARY KEY (code),
    CONSTRAINT tenant_status_code_ck CHECK (code = btrim(code) AND code <> '')
);

INSERT INTO tenant_status (code) VALUES ('ACTIVE'), ('SUSPENDED'), ('OFFBOARDING'), ('OFFBOARDED')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS tenant_status_transition (
    from_status VARCHAR(16) NOT NULL,
    to_status VARCHAR(16) NOT NULL,
    CONSTRAINT pk_tenant_status_transition PRIMARY KEY (from_status, to_status),
    CONSTRAINT tenant_status_transition_from_status_fk FOREIGN KEY (from_status) REFERENCES tenant_status (code) ON DELETE RESTRICT,
    CONSTRAINT tenant_status_transition_to_status_fk FOREIGN KEY (to_status) REFERENCES tenant_status (code) ON DELETE RESTRICT,
    CONSTRAINT tenant_status_transition_distinct_ck CHECK (from_status <> to_status)
);

CREATE INDEX IF NOT EXISTS idx_tenant_status_transition_to_status
    ON tenant_status_transition (to_status);

INSERT INTO tenant_status_transition (from_status, to_status) VALUES
    ('ACTIVE', 'SUSPENDED'),
    ('ACTIVE', 'OFFBOARDING'),
    ('SUSPENDED', 'ACTIVE'),
    ('SUSPENDED', 'OFFBOARDING'),
    ('OFFBOARDING', 'OFFBOARDED')
ON CONFLICT (from_status, to_status) DO NOTHING;

CREATE TABLE IF NOT EXISTS tenant_registry (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    correlation_id UUID NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    suspension_reason VARCHAR(16),
    max_concurrent_sagas INTEGER NOT NULL,
    status_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_tenant_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT tenant_registry_tenant_id_uk UNIQUE (tenant_id),
    CONSTRAINT tenant_registry_public_id_uk UNIQUE (public_id),
    CONSTRAINT tenant_registry_tenant_id_format_ck CHECK (tenant_id ~ '^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$'),
    CONSTRAINT tenant_registry_public_id_ck CHECK (public_id ~ '^tnt_[0-9a-zA-Z]{26}$'),
    CONSTRAINT tenant_registry_status_fk FOREIGN KEY (status) REFERENCES tenant_status (code) ON DELETE RESTRICT,
    CONSTRAINT tenant_registry_suspension_reason_value_ck CHECK (suspension_reason IS NULL OR suspension_reason IN ('BILLING', 'ABUSE')),
    CONSTRAINT tenant_registry_suspension_reason_ck CHECK ((status = 'SUSPENDED') = (suspension_reason IS NOT NULL)),
    CONSTRAINT tenant_registry_max_concurrent_sagas_ck CHECK (max_concurrent_sagas BETWEEN 1 AND 1000000),
    CONSTRAINT tenant_registry_version_ck CHECK (version >= 1)
);

CREATE INDEX IF NOT EXISTS idx_tenant_registry_status
    ON tenant_registry (status);

CREATE TABLE IF NOT EXISTS tenant_tombstone (
    tenant_id VARCHAR(64) NOT NULL,
    tombstoned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_tenant_tombstone PRIMARY KEY (tenant_id),
    CONSTRAINT tenant_tombstone_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS tenant_saga_quota (
    tenant_id VARCHAR(64) NOT NULL,
    active_saga_count BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_tenant_saga_quota PRIMARY KEY (tenant_id),
    CONSTRAINT tenant_saga_quota_tenant_id_fk FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT tenant_saga_quota_active_saga_count_ck CHECK (active_saga_count >= 0),
    CONSTRAINT tenant_saga_quota_version_ck CHECK (version >= 1)
);

CREATE TABLE IF NOT EXISTS tenant_message_quarantine (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    correlation_id UUID NOT NULL,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    reason VARCHAR(24) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    envelope_tenant_id VARCHAR(64),
    key_tenant_id VARCHAR(64),
    payload_tenant_id VARCHAR(64),
    aggregate_tenant_id VARCHAR(64),
    payload_sha256 CHAR(64) NOT NULL,
    resolved_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_tenant_message_quarantine PRIMARY KEY (id),
    CONSTRAINT tenant_message_quarantine_public_id_uk UNIQUE (public_id),
    CONSTRAINT tenant_message_quarantine_source_uk UNIQUE (source_topic, source_partition, source_offset),
    CONSTRAINT tenant_message_quarantine_public_id_ck CHECK (public_id ~ '^tqm_[0-9a-zA-Z]{26}$'),
    CONSTRAINT tenant_message_quarantine_source_topic_ck CHECK (source_topic = btrim(source_topic) AND source_topic <> ''),
    CONSTRAINT tenant_message_quarantine_source_partition_ck CHECK (source_partition >= 0),
    CONSTRAINT tenant_message_quarantine_source_offset_ck CHECK (source_offset >= 0),
    CONSTRAINT tenant_message_quarantine_reason_ck CHECK (reason IN ('UNKNOWN_TENANT', 'INTEGRITY_MISMATCH', 'OFFBOARDED_TENANT')),
    CONSTRAINT tenant_message_quarantine_status_ck CHECK (status IN ('PENDING', 'RELEASED', 'DISCARDED')),
    CONSTRAINT tenant_message_quarantine_payload_sha256_ck CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT tenant_message_quarantine_resolved_ck CHECK ((status = 'PENDING') = (resolved_at IS NULL)),
    CONSTRAINT tenant_message_quarantine_envelope_tenant_id_ck CHECK (envelope_tenant_id IS NULL OR (envelope_tenant_id = btrim(envelope_tenant_id) AND envelope_tenant_id <> '')),
    CONSTRAINT tenant_message_quarantine_version_ck CHECK (version >= 1)
);

CREATE INDEX IF NOT EXISTS idx_tenant_message_quarantine_pending_unknown
    ON tenant_message_quarantine (envelope_tenant_id, created_at, id)
    WHERE status = 'PENDING' AND reason = 'UNKNOWN_TENANT';

CREATE TABLE IF NOT EXISTS tenant_security_event (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    actor_tenant_id VARCHAR(64) NOT NULL,
    referenced_tenant_id VARCHAR(64),
    correlation_id UUID NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    resource_kind VARCHAR(64) NOT NULL,
    resource_id VARCHAR(128),
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_tenant_security_event PRIMARY KEY (actor_tenant_id, id),
    CONSTRAINT tenant_security_event_id_uk UNIQUE (id),
    CONSTRAINT tenant_security_event_public_id_uk UNIQUE (public_id),
    CONSTRAINT tenant_security_event_public_id_ck CHECK (public_id ~ '^tse_[0-9a-zA-Z]{26}$'),
    CONSTRAINT tenant_security_event_actor_tenant_id_fk FOREIGN KEY (actor_tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT tenant_security_event_type_ck CHECK (event_type IN ('CROSS_TENANT_REFERENCE', 'TENANT_SELECTION_REJECTED', 'MESSAGE_INTEGRITY_MISMATCH')),
    CONSTRAINT tenant_security_event_resource_kind_ck CHECK (resource_kind = btrim(resource_kind) AND resource_kind <> ''),
    CONSTRAINT tenant_security_event_resource_id_ck CHECK (resource_id IS NULL OR (resource_id = btrim(resource_id) AND resource_id <> ''))
);

CREATE INDEX IF NOT EXISTS idx_tenant_security_event_occurred
    ON tenant_security_event (actor_tenant_id, occurred_at DESC);

CREATE OR REPLACE FUNCTION tenant_isolation_guard_registry_insert() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
AS $$
BEGIN
    IF NEW.status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'tenant must be registered as ACTIVE'
            USING ERRCODE = '23514', CONSTRAINT = 'tenant_registry_initial_status_ck';
    END IF;
    IF EXISTS (SELECT 1 FROM tenant_tombstone WHERE tenant_id = NEW.tenant_id) THEN
        RAISE EXCEPTION 'tenant id % was offboarded and can never be reissued', NEW.tenant_id
            USING ERRCODE = '23514', CONSTRAINT = 'tenant_registry_tenant_id_tombstone_ck';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION tenant_isolation_guard_registry_transition() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        IF NOT EXISTS (
            SELECT 1 FROM tenant_status_transition
            WHERE from_status = OLD.status AND to_status = NEW.status
        ) THEN
            RAISE EXCEPTION 'tenant status transition % -> % is not allowed', OLD.status, NEW.status
                USING ERRCODE = '23514', CONSTRAINT = 'tenant_registry_status_transition_ck';
        END IF;
        NEW.status_changed_at := NOW();
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION tenant_isolation_record_tombstone() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
AS $$
BEGIN
    IF NEW.status = 'OFFBOARDED' AND OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO tenant_tombstone (tenant_id) VALUES (NEW.tenant_id) ON CONFLICT (tenant_id) DO NOTHING;
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS tenant_registry_guard_insert_tr ON tenant_registry;
CREATE TRIGGER tenant_registry_guard_insert_tr BEFORE INSERT ON tenant_registry
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_registry_insert();

DROP TRIGGER IF EXISTS tenant_registry_guard_immutable_tr ON tenant_registry;
CREATE TRIGGER tenant_registry_guard_immutable_tr BEFORE UPDATE ON tenant_registry
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_immutable('id', 'public_id', 'tenant_id', 'correlation_id');

DROP TRIGGER IF EXISTS tenant_registry_guard_transition_tr ON tenant_registry;
CREATE TRIGGER tenant_registry_guard_transition_tr BEFORE UPDATE ON tenant_registry
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_registry_transition();

DROP TRIGGER IF EXISTS tenant_registry_touch_updated_at_tr ON tenant_registry;
CREATE TRIGGER tenant_registry_touch_updated_at_tr BEFORE UPDATE ON tenant_registry
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_touch_updated_at();

DROP TRIGGER IF EXISTS tenant_registry_record_tombstone_tr ON tenant_registry;
CREATE TRIGGER tenant_registry_record_tombstone_tr AFTER UPDATE ON tenant_registry
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_record_tombstone();

DROP TRIGGER IF EXISTS tenant_registry_reject_delete_tr ON tenant_registry;
CREATE TRIGGER tenant_registry_reject_delete_tr BEFORE DELETE ON tenant_registry
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_reject_mutation();

DROP TRIGGER IF EXISTS tenant_tombstone_reject_update_tr ON tenant_tombstone;
CREATE TRIGGER tenant_tombstone_reject_update_tr BEFORE UPDATE OR DELETE ON tenant_tombstone
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_reject_mutation();

DROP TRIGGER IF EXISTS tenant_saga_quota_guard_immutable_tr ON tenant_saga_quota;
CREATE TRIGGER tenant_saga_quota_guard_immutable_tr BEFORE UPDATE ON tenant_saga_quota
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_immutable('tenant_id');

DROP TRIGGER IF EXISTS tenant_saga_quota_touch_updated_at_tr ON tenant_saga_quota;
CREATE TRIGGER tenant_saga_quota_touch_updated_at_tr BEFORE UPDATE ON tenant_saga_quota
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_touch_updated_at();

DROP TRIGGER IF EXISTS tenant_message_quarantine_guard_immutable_tr ON tenant_message_quarantine;
CREATE TRIGGER tenant_message_quarantine_guard_immutable_tr BEFORE UPDATE ON tenant_message_quarantine
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_guard_immutable(
        'id', 'public_id', 'correlation_id', 'source_topic', 'source_partition', 'source_offset',
        'reason', 'envelope_tenant_id', 'key_tenant_id', 'payload_tenant_id', 'aggregate_tenant_id', 'payload_sha256');

DROP TRIGGER IF EXISTS tenant_message_quarantine_touch_updated_at_tr ON tenant_message_quarantine;
CREATE TRIGGER tenant_message_quarantine_touch_updated_at_tr BEFORE UPDATE ON tenant_message_quarantine
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_touch_updated_at();

DROP TRIGGER IF EXISTS tenant_message_quarantine_reject_delete_tr ON tenant_message_quarantine;
CREATE TRIGGER tenant_message_quarantine_reject_delete_tr BEFORE DELETE ON tenant_message_quarantine
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_reject_mutation();

DROP TRIGGER IF EXISTS tenant_security_event_reject_mutation_tr ON tenant_security_event;
CREATE TRIGGER tenant_security_event_reject_mutation_tr BEFORE UPDATE OR DELETE ON tenant_security_event
    FOR EACH ROW EXECUTE FUNCTION tenant_isolation_reject_mutation();

REVOKE ALL ON tenant_status, tenant_status_transition, tenant_registry, tenant_tombstone,
    tenant_saga_quota, tenant_message_quarantine, tenant_security_event FROM PUBLIC;

GRANT SELECT ON tenant_status, tenant_status_transition TO dtx_app, dtx_platform;
GRANT SELECT ON tenant_registry TO dtx_app;
GRANT SELECT, INSERT, UPDATE ON tenant_registry TO dtx_platform;
GRANT SELECT, INSERT ON tenant_tombstone TO dtx_platform;
GRANT SELECT, INSERT, UPDATE ON tenant_saga_quota TO dtx_app, dtx_platform;
GRANT INSERT ON tenant_message_quarantine TO dtx_app;
GRANT SELECT, INSERT, UPDATE ON tenant_message_quarantine TO dtx_platform;
GRANT INSERT ON tenant_security_event TO dtx_app;
GRANT SELECT, INSERT ON tenant_security_event TO dtx_platform;

ALTER TABLE tenant_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_registry FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_tombstone ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_tombstone FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_saga_quota ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_saga_quota FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_message_quarantine ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_message_quarantine FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_security_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_security_event FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_registry_app_read ON tenant_registry;
CREATE POLICY tenant_registry_app_read ON tenant_registry FOR SELECT TO dtx_app
    USING (tenant_id = tenant_isolation_current_tenant());

DROP POLICY IF EXISTS tenant_registry_platform_all ON tenant_registry;
CREATE POLICY tenant_registry_platform_all ON tenant_registry FOR ALL TO dtx_platform
    USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tenant_tombstone_platform_all ON tenant_tombstone;
CREATE POLICY tenant_tombstone_platform_all ON tenant_tombstone FOR ALL TO dtx_platform
    USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tenant_saga_quota_app_isolation ON tenant_saga_quota;
CREATE POLICY tenant_saga_quota_app_isolation ON tenant_saga_quota FOR ALL TO dtx_app
    USING (tenant_id = tenant_isolation_current_tenant())
    WITH CHECK (tenant_id = tenant_isolation_current_tenant());

DROP POLICY IF EXISTS tenant_saga_quota_platform_all ON tenant_saga_quota;
CREATE POLICY tenant_saga_quota_platform_all ON tenant_saga_quota FOR ALL TO dtx_platform
    USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tenant_message_quarantine_app_insert ON tenant_message_quarantine;
CREATE POLICY tenant_message_quarantine_app_insert ON tenant_message_quarantine FOR INSERT TO dtx_app
    WITH CHECK (true);

DROP POLICY IF EXISTS tenant_message_quarantine_platform_all ON tenant_message_quarantine;
CREATE POLICY tenant_message_quarantine_platform_all ON tenant_message_quarantine FOR ALL TO dtx_platform
    USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS tenant_security_event_app_insert ON tenant_security_event;
CREATE POLICY tenant_security_event_app_insert ON tenant_security_event FOR INSERT TO dtx_app
    WITH CHECK (actor_tenant_id = tenant_isolation_current_tenant());

DROP POLICY IF EXISTS tenant_security_event_platform_all ON tenant_security_event;
CREATE POLICY tenant_security_event_platform_all ON tenant_security_event FOR ALL TO dtx_platform
    USING (true) WITH CHECK (true);

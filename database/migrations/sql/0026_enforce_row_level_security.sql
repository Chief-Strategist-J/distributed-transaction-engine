-- migration:      0026
-- description:    enforce tenant row level security on every tenant scoped table and grant least privilege DML to dtx_app and dtx_platform
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0022_create_tenant_isolation_tables.sql
-- reversible:     YES
-- lock_risk:      MEDIUM
-- rows_affected:  schema only
-- lock_impact:    ALTER TABLE ENABLE/FORCE ROW LEVEL SECURITY takes a brief ACCESS EXCLUSIVE lock per table; CREATE POLICY takes SHARE ROW EXCLUSIVE
-- data_check:     SELECT c.relname FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'tenant_id' WHERE c.relkind IN ('r','p') AND c.relnamespace = 'public'::regnamespace AND NOT c.relispartition AND NOT (c.relrowsecurity AND c.relforcerowsecurity)

GRANT USAGE ON SCHEMA public TO dtx_app, dtx_platform;

DO $$
DECLARE
    target RECORD;
BEGIN
    FOR target IN
        SELECT c.relname
        FROM pg_class c
        WHERE c.relnamespace = 'public'::regnamespace
          AND c.relkind IN ('r', 'p')
          AND NOT c.relispartition
          AND NOT c.relrowsecurity
          AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'tenant_id' AND NOT a.attisdropped)
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', target.relname);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', target.relname);
        EXECUTE format('CREATE POLICY %I ON %I FOR ALL TO dtx_app USING (tenant_id = tenant_isolation_current_tenant()) WITH CHECK (tenant_id = tenant_isolation_current_tenant())', target.relname || '_rls_tenant_pol', target.relname);
        EXECUTE format('CREATE POLICY %I ON %I FOR ALL TO dtx_platform USING (true) WITH CHECK (true)', target.relname || '_rls_platform_pol', target.relname);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I TO dtx_app, dtx_platform', target.relname);
    END LOOP;
END
$$;

DO $$
DECLARE
    reference RECORD;
BEGIN
    FOR reference IN
        SELECT c.relname
        FROM pg_class c
        WHERE c.relnamespace = 'public'::regnamespace
          AND c.relkind IN ('r', 'p')
          AND NOT c.relispartition
          AND c.relname <> 'schema_migrations'
          AND NOT EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'tenant_id' AND NOT a.attisdropped)
    LOOP
        EXECUTE format('GRANT SELECT ON %I TO dtx_app, dtx_platform', reference.relname);
    END LOOP;
END
$$;

GRANT SELECT, INSERT, UPDATE ON tq_watermark TO dtx_platform;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO dtx_app, dtx_platform;

DO $$
DECLARE
    violations BIGINT;
BEGIN
    SELECT COUNT(*) INTO violations
    FROM pg_class c
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relkind IN ('r', 'p')
      AND NOT c.relispartition
      AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'tenant_id' AND NOT a.attisdropped)
      AND NOT (c.relrowsecurity AND c.relforcerowsecurity);
    IF violations > 0 THEN
        RAISE EXCEPTION 'row level security is still missing on % tenant scoped tables', violations;
    END IF;
END
$$;

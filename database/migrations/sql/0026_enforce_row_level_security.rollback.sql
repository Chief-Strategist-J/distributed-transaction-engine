-- migration:      0026
-- description:    rollback generic tenant row level security policies and grants created by 0026
-- author:         lead-architect
-- date:           2026-11-11T00:00:00Z
-- depends_on:     0026_enforce_row_level_security.sql
-- reversible:     YES
-- lock_risk:      MEDIUM
-- rows_affected:  schema only

DO $$
DECLARE
    target RECORD;
BEGIN
    FOR target IN
        SELECT schemaname, tablename FROM pg_policies
        WHERE schemaname = 'public' AND policyname = tablename || '_rls_tenant_pol'
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS %I ON %I', target.tablename || '_rls_tenant_pol', target.tablename);
        EXECUTE format('DROP POLICY IF EXISTS %I ON %I', target.tablename || '_rls_platform_pol', target.tablename);
        EXECUTE format('ALTER TABLE %I NO FORCE ROW LEVEL SECURITY', target.tablename);
        EXECUTE format('ALTER TABLE %I DISABLE ROW LEVEL SECURITY', target.tablename);
        EXECUTE format('REVOKE ALL ON %I FROM dtx_app, dtx_platform', target.tablename);
    END LOOP;
END
$$;

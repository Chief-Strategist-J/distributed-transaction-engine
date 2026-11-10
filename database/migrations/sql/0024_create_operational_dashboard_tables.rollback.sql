-- migration:      0024
-- description:    rollback operational dashboard stream sessions, tile snapshots, action previews, and audit events
-- author:         lead-architect
-- date:           2026-11-10T14:30:00Z
-- depends_on:     0024_create_operational_dashboard_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only

DROP INDEX IF EXISTS idx_dashboard_audit_events_operator;
DROP INDEX IF EXISTS idx_dashboard_action_previews_saga;
DROP INDEX IF EXISTS idx_dashboard_tile_snapshots_tenant;
DROP INDEX IF EXISTS idx_dashboard_stream_sessions_tenant;

DROP TABLE IF EXISTS dashboard_audit_events CASCADE;
DROP TABLE IF EXISTS dashboard_action_previews CASCADE;
DROP TABLE IF EXISTS dashboard_tile_snapshots CASCADE;
DROP TABLE IF EXISTS dashboard_stream_sessions CASCADE;


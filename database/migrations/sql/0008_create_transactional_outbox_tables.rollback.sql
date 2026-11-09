-- Migration: 0008_create_transactional_outbox_tables.rollback.sql
-- Direction: Down

DROP TABLE IF EXISTS outbox_events CASCADE;

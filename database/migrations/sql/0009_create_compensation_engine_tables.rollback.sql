-- Migration: 0009_create_compensation_engine_tables.rollback.sql
-- Direction: Down

DROP TABLE IF EXISTS saga_step_compensations CASCADE;
DROP TABLE IF EXISTS saga_compensation_plans CASCADE;

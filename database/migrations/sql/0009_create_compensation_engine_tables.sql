-- Migration: 0009_create_compensation_engine_tables.sql
-- Direction: Up

CREATE TABLE IF NOT EXISTS saga_compensation_plans (
    id UUID PRIMARY KEY,
    public_id VARCHAR(30) NOT NULL UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    saga_id UUID NOT NULL,
    current_state VARCHAR(40) NOT NULL DEFAULT 'COMPENSATING',
    total_steps INT NOT NULL DEFAULT 0,
    compensated_steps INT NOT NULL DEFAULT 0,
    failed_steps INT NOT NULL DEFAULT 0,
    residuals JSONB NOT NULL DEFAULT '[]'::jsonb,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT saga_compensation_plans_public_id_ck CHECK (public_id ~ '^cmp_[0-9a-zA-Z]{26}$'),
    CONSTRAINT saga_compensation_plans_state_ck CHECK (current_state IN (
        'COMPENSATING', 'FULLY_COMPENSATED', 'COMPENSATED_WITH_RESIDUALS', 'COMPENSATING_NEEDS_ATTENTION'
    )),
    CONSTRAINT saga_compensation_plans_residuals_json_ck CHECK (jsonb_typeof(residuals) = 'array'),
    CONSTRAINT saga_compensation_plans_unique_saga UNIQUE (tenant_id, saga_id)
);

CREATE TABLE IF NOT EXISTS saga_step_compensations (
    id UUID PRIMARY KEY,
    public_id VARCHAR(30) NOT NULL UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    plan_id UUID NOT NULL REFERENCES saga_compensation_plans(id) ON DELETE CASCADE,
    saga_id UUID NOT NULL,
    step_id VARCHAR(128) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    execution_order INT NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    forward_execution_id VARCHAR(128) NOT NULL,
    compensation_action VARCHAR(255) NOT NULL,
    compensation_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    residual_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key VARCHAR(255) NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    compensated_at TIMESTAMPTZ,

    CONSTRAINT saga_step_compensations_public_id_ck CHECK (public_id ~ '^cst_[0-9a-zA-Z]{26}$'),
    CONSTRAINT saga_step_compensations_status_ck CHECK (status IN (
        'PENDING', 'DISPATCHED', 'COMPENSATED', 'NOT_COMPENSABLE', 'PARTIALLY_COMPENSATED', 'FAILED_RETRYABLE', 'NEEDS_ATTENTION'
    )),
    CONSTRAINT saga_step_compensations_payload_json_ck CHECK (jsonb_typeof(compensation_payload) = 'object'),
    CONSTRAINT saga_step_compensations_unique_step UNIQUE (tenant_id, plan_id, step_id)
);

CREATE INDEX IF NOT EXISTS idx_compensation_plans_saga ON saga_compensation_plans(tenant_id, saga_id);
CREATE INDEX IF NOT EXISTS idx_step_compensations_plan ON saga_step_compensations(tenant_id, plan_id, execution_order ASC);

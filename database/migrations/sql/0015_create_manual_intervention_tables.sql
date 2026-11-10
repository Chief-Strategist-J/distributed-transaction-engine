-- migration:      0014
-- description:    create manual intervention audit logs and action transitions
-- author:         lead-architect
-- date:           2026-11-10T10:30:00Z
-- depends_on:     0013_create_reconciliation_engine_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide operator auditability and state transition enforcement for manual interventions

CREATE TABLE IF NOT EXISTS manual_intervention_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'MANUAL_OPERATOR_ACTION',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    operator_id VARCHAR(128) NOT NULL,
    operator_role VARCHAR(64) NOT NULL,
    action_type VARCHAR(32) NOT NULL,
    reason TEXT NOT NULL,
    approval_ticket_id VARCHAR(128) NOT NULL DEFAULT '',
    synthetic_payload TEXT NOT NULL DEFAULT '',
    old_state VARCHAR(32) NOT NULL,
    new_state VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_manual_intervention_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT manual_intervention_audit_log_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT manual_intervention_audit_log_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT manual_intervention_audit_log_natural_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT manual_intervention_audit_log_public_id_ck CHECK (public_id ~ '^ops_[0-9a-zA-Z]{26}$'),
    CONSTRAINT manual_intervention_audit_log_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT manual_intervention_audit_log_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT manual_intervention_audit_log_operator_id_ck CHECK (operator_id = btrim(operator_id) AND operator_id <> ''),
    CONSTRAINT manual_intervention_audit_log_action_ck CHECK (action_type IN ('FORCE_RETRY', 'FORCE_COMPENSATE', 'FORCE_COMPLETE', 'FORCE_TERMINATE')),
    CONSTRAINT manual_intervention_audit_log_reason_ck CHECK (reason = btrim(reason) AND reason <> '')
);

CREATE INDEX IF NOT EXISTS idx_manual_intervention_audit_saga
    ON manual_intervention_audit_log (tenant_id, saga_id);

CREATE INDEX IF NOT EXISTS idx_manual_intervention_audit_operator
    ON manual_intervention_audit_log (tenant_id, operator_id);

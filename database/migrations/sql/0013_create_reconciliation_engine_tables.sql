-- migration:      0013
-- description:    create reconciliation anomalies and run records tables
-- author:         lead-architect
-- date:           2026-11-10T10:15:00Z
-- depends_on:     0012_create_compensation_retry_tables.sql
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         provide three-way ledger reconciliation, anomaly detection, and auto-repair tracking

CREATE TABLE IF NOT EXISTS reconciliation_anomaly_transitions (
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_reconciliation_anomaly_transitions PRIMARY KEY (from_state, to_state)
);

INSERT INTO reconciliation_anomaly_transitions (from_state, to_state) VALUES
    ('ANOMALY_DETECTED', 'REPAIR_SCHEDULED'),
    ('ANOMALY_DETECTED', 'AUTO_RESOLVED'),
    ('ANOMALY_DETECTED', 'QUARANTINED_FOR_AUDIT'),
    ('REPAIR_SCHEDULED', 'AUTO_RESOLVED'),
    ('REPAIR_SCHEDULED', 'QUARANTINED_FOR_AUDIT'),
    ('QUARANTINED_FOR_AUDIT', 'AUTO_RESOLVED'),
    ('QUARANTINED_FOR_AUDIT', 'MANUAL_DISMISSED')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS reconciliation_anomalies (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'ENGINE_RECONCILIATION',
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    anomaly_type VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ANOMALY_DETECTED',
    internal_status VARCHAR(32) NOT NULL,
    external_status VARCHAR(32) NOT NULL,
    internal_amount_cents BIGINT NOT NULL DEFAULT 0,
    external_amount_cents BIGINT NOT NULL DEFAULT 0,
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    discrepancy_reason TEXT NOT NULL DEFAULT '',
    resolution_action TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_reconciliation_anomalies PRIMARY KEY (tenant_id, id),
    CONSTRAINT reconciliation_anomalies_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT reconciliation_anomalies_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT reconciliation_anomalies_natural_uk UNIQUE (tenant_id, source, external_id),
    CONSTRAINT reconciliation_anomalies_public_id_ck CHECK (public_id ~ '^rec_[0-9a-zA-Z]{26}$'),
    CONSTRAINT reconciliation_anomalies_source_ck CHECK (source = btrim(source) AND source <> ''),
    CONSTRAINT reconciliation_anomalies_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT reconciliation_anomalies_type_ck CHECK (anomaly_type IN ('PHANTOM_SETTLEMENT', 'GHOST_TRANSACTION', 'NUMERICAL_DRIFT', 'STATUS_MISMATCH')),
    CONSTRAINT reconciliation_anomalies_status_ck CHECK (status IN ('ANOMALY_DETECTED', 'REPAIR_SCHEDULED', 'AUTO_RESOLVED', 'QUARANTINED_FOR_AUDIT', 'MANUAL_DISMISSED')),
    CONSTRAINT reconciliation_anomalies_currency_ck CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT reconciliation_anomalies_amount_nonneg_ck CHECK (internal_amount_cents >= 0 AND external_amount_cents >= 0)
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_anomalies_tenant_status
    ON reconciliation_anomalies (tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_reconciliation_anomalies_saga_id
    ON reconciliation_anomalies (tenant_id, saga_id);

-- rollback:    0011
-- description: drop fencing leases and fencing violation audit tables

DROP TABLE IF EXISTS fencing_audits;
DROP TABLE IF EXISTS fencing_leases;


# Tenant Isolation — Failure Cases, Risk Mitigations & Attack Drills

This document analyzes security attack vectors, connection pool leaks, noisy-neighbor starvation, and defensive mitigations for the Multi-Tenant Isolation Subsystem.

---

## 1. Failure Modes & Security Resilience Matrix

| Attack / Failure ID | Scenario / Vector | Immediate Impact | Invariants Challenged | Architectural Mitigation & Detection |
|---|---|---|---|---|
| **FAIL-TI-001** | **Client-Injected `X-Tenant-ID` Header Spoofing**: An authenticated user belonging to Tenant A sends `X-Tenant-ID: Tenant-B` in request headers. | If middleware trusts the header without cross-checking the JWT token claims, Tenant A reads and writes Tenant B's sagas. | **TI-01** | **Mitigation:** Token-derived authority. The API gateway derives tenant context exclusively from the verified JWT claims (`jwt.claims["tenant_id"]`). `X-Tenant-ID` is strictly ignored unless the user possesses explicit multi-membership grants, and even then must match the cryptographically signed allowed list. |
| **FAIL-TI-002** | **Session-Level `SET` Leak Across Connection Pools**: Backend issues `SET app.current_tenant = 'tenant-A'` on a pooled PgBouncer connection. Connection is returned to pool without explicit reset. | Next transaction for Tenant B borrows connection and inherits Tenant A's RLS permissions, exposing private data. | **TI-03** | **Mitigation:** Transaction-scoped `SET LOCAL`. Context is injected strictly inside an active transaction (`BEGIN; SET LOCAL app.current_tenant = '...'; ...; COMMIT;`). PostgreSQL automatically unsets `SET LOCAL` variables upon transaction commit or rollback, guaranteeing zero pool contamination. |
| **FAIL-TI-003** | **Check-Then-Act Quota Starvation (Noisy Neighbor)**: Tenant A has a quota of 10 concurrent sagas. 50 parallel requests check `SELECT count(*) ...` (all see count = 8) and all 50 proceed to create sagas. | Tenant A consumes 58 worker slots instead of 10, starving other tenants sharing the cluster. | **TI-08** | **Mitigation:** Atomic in-database quota reservation: `UPDATE tenant_quotas SET current_active_sagas = current_active_sagas + 1 WHERE tenant_id = :id AND current_active_sagas < max_active_sagas RETURNING current_active_sagas`. Exactly 2 succeed; 48 receive 0 rows and return `HTTP 429 Too Many Requests`. |
| **FAIL-TI-004** | **Cross-Tenant ID Enumeration via 403 Forbidden**: Tenant A requests `GET /api/v1/sagas/tx-tenant-B-99`. System checks tenant ownership and returns `403 Forbidden`. | Attacker determines that `tx-tenant-B-99` is a valid, existing saga ID in the platform, enabling targeted enumeration. | **TI-05, TI-06** | **Mitigation:** Opaque `404 Not Found`. Any query referencing a resource not owned by the authenticated tenant returns `HTTP 404 Not Found` with an identical body to non-existent IDs. Security logs record the enumeration attempt for SIEM rate-limiting. |
| **FAIL-TI-005** | **Superuser / Table Owner RLS Bypass**: Application connects to PostgreSQL using the table owner or superuser role (`postgres`). | PostgreSQL default behavior automatically bypasses Row-Level Security for table owners and superusers. | **TI-04** | **Mitigation:** Forced RLS and least-privilege runtime roles. Tables enforce `ALTER TABLE saga_instances FORCE ROW LEVEL SECURITY;`. Applications connect via an unprivileged runtime user `dist_tx_app` that does NOT possess `BYPASSRLS` privileges. |
| **FAIL-TI-006** | **Write-Injection via Missing `WITH CHECK` Clause**: RLS policy defines only `USING (tenant_id = current_setting('...'))`. An attacker updates a saga row and sets `tenant_id = 'victim-tenant'`. | A compromised application worker can move rows into another tenant's namespace. | **TI-04** | **Mitigation:** Symmetrical RLS policy. Policies must define both `USING` (filtering reads) and `WITH CHECK (tenant_id = current_setting('app.current_tenant'))` (validating inserted and updated values). |

---

## 2. In-Depth Attack Drill: Header Spoofing Interception

```mermaid
sequenceDiagram
    autonumber
    participant Attacker as Attacker (Tenant A Credential)
    participant Gateway as API Gateway
    participant Auth as JWT Validator
    participant DB as PostgreSQL 18

    Attacker->>Gateway: POST /sagas (Header: X-Tenant-ID: Tenant-B, Auth: Bearer JWT_TenantA)
    Gateway->>Auth: Validate JWT Signature
    Auth-->>Gateway: Claims: {sub: 'usr-12', tenant_id: 'Tenant-A'}
    
    rect rgb(255, 240, 240)
        Note over Gateway: Invariant TI-01 Check
        Gateway->>Gateway: Discard unverified X-Tenant-ID header
        Gateway->>Gateway: Force TenantContext = 'Tenant-A'
    end

    Gateway->>DB: BEGIN; SET LOCAL app.current_tenant = 'Tenant-A';
    Gateway->>DB: INSERT INTO saga_instances (tenant_id, id) VALUES ('Tenant-B', 'tx-1')
    
    rect rgb(255, 230, 230)
        Note over DB: Invariant TI-04: WITH CHECK Violation
        DB-->>Gateway: ERROR: new row violates row-level security policy for table "saga_instances"
    end

    Gateway-->>Attacker: HTTP 403 Forbidden: Cross-Tenant Context Violation
```

---

## 3. Operator Runbook: Mitigating Noisy Neighbor Starvation

When a tenant triggers `TenantQuotaBurstExceeded` alerts:

1. **Query top tenants by active saga count and database lock contention:**
   ```sql
   SELECT tenant_id, current_active_sagas, max_active_sagas, burst_allowance, updated_at
   FROM tenant_quotas
   ORDER BY current_active_sagas DESC
   LIMIT 10;
   ```
2. **Temporarily clamp quota for abusive tenant:**
   ```sql
   UPDATE tenant_quotas 
   SET max_active_sagas = 25, burst_allowance = 0 
   WHERE tenant_id = 'abusive_tenant_id';
   ```
3. **If malicious activity is confirmed, apply soft suspension:**
   ```sql
   UPDATE tenant_quotas 
   SET is_suspended = TRUE, suspension_reason = 'EXCESSIVE_RATE_ABUSE' 
   WHERE tenant_id = 'abusive_tenant_id';
   ```


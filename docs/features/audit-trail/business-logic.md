Assigning **"SYSTEM_KERNEL" / "GLOBAL_SYSTEM"** when actor or tenant is missing turns a bug into a **falsified audit record**:
- An operator's manual refund whose identity got lost in propagation is permanently recorded as "the system did it."
- The tenant's event lands in a **global** chain, so the tenant's own chain is silently incomplete and its isolation is broken.

An audit trail's first job is attribution. Missing attribution must be **quarantined and fixed at the producer**, never filled with a default. Genuine system actions must be declared explicitly by the producer, with job identity. [Certain]

Five more errors, ranked by damage:

**1. "Lock tenant audit ledger" on a hash mismatch stops auditing exactly when it matters most.** [Certain]
I disagree because once the ledger is locked, every subsequent business action for that tenant either blocks (outage) or proceeds **unaudited**. It's also a denial-of-service lever: anyone able to corrupt one row can disable auditing for a whole tenant.
Here's what I'd do instead: preserve evidence, seal the damaged segment, and **continue appending on a new segment** linked to the last verified checkpoint, under an incident.

**2. Per-tenant chaining collides with the business transaction either way.** [Certain]
I disagree because the design has no good placement for the chain write:
- **Inside the business transaction:** fetching the tenant's tail hash serializes **every business transaction of that tenant** behind one row. Hot tenants collapse into conflict-retry storms.
- **In a separate transaction:** you get committed mutations with no audit, or audit records for rolled-back mutations.

Here's what I'd do instead: write an **unchained audit record in the business transaction** (audit outbox), and have a **single chainer per tenant shard** assign sequence and hash afterward.

**3. "Epoch millis + counter" is not a monotonic sequence.** [Certain]
Clocks differ across hosts and can step backward under NTP corrections. Counters reset on restart within the same millisecond. You get duplicates and out-of-order IDs. The chain's order must come from a **sequence assigned by the chainer**.

**4. The hash input is ambiguous, and the hash is unkeyed.** [Certain]
Plain concatenation (`a || b`) without length prefixes means different field splits produce identical input: "ab"+"c" hashes the same as "a"+"bc". JSON payloads also re-serialize differently. Worse, an unkeyed SHA-256 chain stored in the same database can be **recomputed by anyone who can write the table**, so verification still passes after tampering.
Here's what I'd do instead: canonical length-prefixed encoding, a keyed hash (key outside the database), and **signed external checkpoints**.

**5. Schema-invalid audit events go to a dead-letter queue and out of the chain.** [Certain]
Those are records of real actions, and a producer bug now erases them from the evidence. Invalid events must be **retained raw**, reconciled, and chained once repaired. For financial actions, capture should fail closed.

```
AUDIT TRAIL (SAGA ENGINE) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (compliance/legal must decide)
│
├── 0. WHAT THE CHAIN PROVES (AND DOES NOT)
│   ├── Proves: records in a verified segment were not altered, inserted, reordered, or removed —
│   │   relative to an externally anchored checkpoint
│   ├── Does NOT prove: that every action was captured (completeness → §2 + reconciliation)
│   ├── Does NOT prove: attribution correctness if producers send wrong actors (§2.3)
│   └── E0.1 "Chain valid" reported as "audit complete" — missing captures go unnoticed [C]
│
├── 1. INVARIANTS
│   ├── AT-01 Audit capture for state-changing actions commits in the same transaction as the action [C]
│   ├── AT-02 Every record has explicit actor and tenant; missing values are never defaulted [C]
│   ├── AT-03 Records are never dropped; invalid records are quarantined raw and reconciled [C]
│   ├── AT-04 Chain order is assigned by a single chainer per chain; never by producer clocks [C]
│   ├── AT-05 Hash input is canonical, length-prefixed, and keyed (key outside the database) [C]
│   ├── AT-06 Chain heads are anchored externally as signed checkpoints on a schedule [C]
│   ├── AT-07 Tamper detection never stops auditing; it seals and continues on a new segment [C]
│   ├── AT-08 Duplicates are removed by audit_id before chaining [C]
│   └── AT-09 Access to audit data is scoped, masked, and itself audited [C]
│
├── 2. CAPTURE
│   ├── 2.1 Placement
│   │   ├── State-changing actions (saga transitions, operator interventions, compensations, approvals)
│   │   │   → audit outbox row in the SAME transaction (AT-01)
│   │   ├── Actions without a transaction (logins, denied attempts, reads of sensitive data)
│   │   │   → durable direct write; fail policy per class (financial/admin: fail closed)
│   │   └── Each record: audit_id (deterministic from action instance), occurred_at (source), action, outcome,
│   │       actor, on_behalf_of chain, tenant, target, correlation/trace ids, payload (canonical, PII encrypted)
│   ├── 2.2 Tenant (AT-02)
│   │   ├── From the action's aggregate/tenant context, never from request headers
│   │   ├── Missing → QUARANTINE (ATTRIBUTION_MISSING) + producer defect alert
│   │   └── Platform-level actions (no tenant) → explicit platform chain, declared by producer
│   ├── 2.3 Actor (AT-02)
│   │   ├── Human: principal id, session, authentication method; impersonation recorded as actor + on_behalf_of
│   │   ├── Service on behalf of user: service identity + delegation chain
│   │   ├── Scheduled/system job: job identity + run id + triggering schedule/approval (declared by producer)
│   │   └── Missing → QUARANTINE (never "SYSTEM_KERNEL")
│   └── Edge cases
│       ├── E2.1 Operator action recorded as system because identity header lost — falsified attribution — COMPLIANCE [C]
│       ├── E2.2 Tenant missing → event filed under global chain — tenant chain incomplete — COMPLIANCE [C]
│       ├── E2.3 Audit written after commit in separate step — crash → action without audit — LOSS [C]
│       ├── E2.4 Audit written before commit, action rolls back — audit of action that never happened — CORRUPT [C]
│       │   └── Handling: only via same-transaction outbox; or INTENT + OUTCOME pairs for non-transactional actions
│       └── E2.5 Retried command emits a second audit record — duplicate actions in evidence — CORRUPT [C]
│           └── Handling: audit_id derived from idempotency key / action instance; dedupe (AT-08)
│
├── 3. VALIDATION & QUARANTINE  (AT-03)
│   ├── Schema validation at chainer ingestion
│   │   ├── Valid → chain
│   │   └── Invalid → QUARANTINED with raw bytes, reason, producer, time; NOT chained yet
│   ├── Quarantine resolution: producer fix → record repaired via documented transformation (original raw retained)
│   │   → chained with "repaired" marker and reference to raw
│   ├── Quarantine age alert; quarantine counts per producer
│   └── Edge cases
│       ├── E3.1 Invalid events discarded via DLQ — evidence of real actions lost — COMPLIANCE [C]
│       └── E3.2 Repair edits record without keeping raw — undetectable rewrite of evidence [C]
│
├── 4. CHAINING  (AT-04, AT-05, AT-08)
│   ├── Chain scope: per tenant (or per tenant shard for very high volume), plus a platform chain
│   ├── Single chainer per chain (lease + fencing epoch); reads audit outbox in commit order
│   ├── Steps (one transaction)
│   │   ├── Dedupe by audit_id (already chained → skip)
│   │   ├── Assign next chain sequence (gapless per chain)
│   │   ├── Canonical encoding of record: length-prefixed fields, fixed field order, normalized timestamps/numbers,
│   │   │   payload canonical bytes stored verbatim (not re-normalized storage types)
│   │   ├── Hash = keyed hash(key id, prev hash, canonical record) — key held in KMS/HSM
│   │   └── Append chained row; mark outbox row chained
│   ├── Business transactions never touch the chain tail (no per-tenant lock in business path)
│   └── Edge cases
│       ├── E4.1 Chain tail fetched inside business tx — tenant-wide serialization — throughput collapse [C]
│       ├── E4.2 Two chainers for one chain (lease lost, zombie) — forked chain [C]
│       │   └── Handling: fencing epoch on chain head update; second chainer's append fails
│       ├── E4.3 Plain concatenation hash — field-boundary ambiguity — two different records same hash input [C]
│       ├── E4.4 Payload stored as normalized JSON type — stored bytes ≠ hashed bytes — verification fails on everything [C]
│       └── E4.5 Hash key rotated without recording key id per record — old segments unverifiable [C]
│
├── 5. SEQUENCE & TIME SEMANTICS
│   ├── chain_sequence: order of the evidence (authoritative)
│   ├── occurred_at: source time (may be late, skewed) — shown, not used for order
│   ├── chained_at: chainer database time
│   ├── Late arrivals (delayed outbox, quarantine repair) → chained later; flagged with lateness
│   └── E5.1 Producer clock + counter used as sequence — duplicates/out-of-order — forensic timeline wrong [C]
│
├── 6. ANCHORING & WORM COPY  (AT-06)
│   ├── Checkpoint every interval [L: 5–15 min] per chain: (chain id, last sequence, head hash, record count)
│   ├── Checkpoint signed with key in KMS/HSM; written to WORM storage and optionally external timestamping
│   ├── WORM archive copy of chained segments produced from committed data (export job / outbox), with manifest
│   ├── Anchor publication failure → alert; unanchored window tracked
│   └── Edge cases
│       ├── E6.1 Async forward to WORM after commit fails silently — archive incomplete — evidence gap [C]
│       ├── E6.2 Insider deletes latest rows — chain still internally valid — undetectable without anchors [C]
│       └── E6.3 Insider rewrites table and recomputes unkeyed hashes — verification passes — undetectable [C]
│
├── 7. VERIFICATION
│   ├── Incremental: from last verified checkpoint to head, continuously
│   ├── Full: per chain on schedule, segment by segment between anchored checkpoints (parallel across chains)
│   ├── Checks: keyed hash recomputation, prev-hash linkage, sequence gaplessness, head vs latest anchor
│   │   (truncation/rollback), archive copy vs database segment
│   ├── Results: VALID | INVALID(segment, first bad sequence) | UNKNOWN (source/archive unavailable — never reported VALID)
│   ├── Pruned hot segments verified against archive + anchors (not from genesis in database)
│   └── Edge cases
│       ├── E7.1 Full re-verification from genesis per request — infeasible at scale; checks skipped — false assurance [C]
│       └── E7.2 Verification during appends reads moving head — false mismatch [C]
│           └── Handling: verify up to a fixed checkpoint, not live head
│
├── 8. TAMPER RESPONSE  (AT-07)
│   ├── On INVALID
│   │   ├── Preserve evidence: snapshot affected segment, database logs, access logs
│   │   ├── Seal segment (marked compromised from first bad sequence)
│   │   ├── Continue: new segment starts linked to last valid anchored checkpoint + incident marker record
│   │   ├── Restore content from WORM archive where available (records), keeping both versions for investigation
│   │   └── Security incident; access review of audit storage
│   └── Edge cases
│       ├── E8.1 Ledger locked on mismatch — subsequent actions unaudited or blocked — OUTAGE/COMPLIANCE [C]
│       └── E8.2 Attacker corrupts one row to trigger lock — audit disabled for tenant — SECURITY [C]
│
├── 9. ACCESS & QUERY  (AT-09)
│   ├── Roles: auditor (masked, tenant-scoped), investigator (case-bound unmask), customer (own tenant view)
│   ├── Every query recorded as an audit record in the platform chain (who, scope, purpose)
│   ├── Exports include records, proofs (sequence ranges, head hashes), signed checkpoints, verification result
│   └── E9.1 "audit:read" grants all tenants' raw payloads — data exposure — SECURITY [C]
│
├── 10. PRIVACY & ERASURE
│   ├── Personal fields encrypted per data subject before chaining; hash covers ciphertext
│   ├── Erasure = destroy subject key → content unreadable, chain still verifiable
│   ├── Actor identifiers pseudonymized where law requires [G]
│   └── E10.1 Plaintext PII in chained payload — erasure impossible without breaking chain — COMPLIANCE [C]
│
├── 11. MULTI-REGION & DR
│   ├── Each chain has one home region (single chainer); other regions forward records to it or keep regional chains
│   ├── Failover: chainer ownership moves with epoch bump; new chainer resumes from last committed head
│   ├── Anchors replicated to independent storage/account
│   └── E11.1 Both regions chain the same tenant during split brain — forked chains — evidence ambiguity [C]
│
└── 12. FEATURE TEST SCENARIOS
    ├── Saga transition commits → exactly one audit record chained; rollback → none
    ├── Event without actor → quarantined, producer alerted; nothing attributed to system
    ├── Explicit system job event → chained with job identity and run id
    ├── Schema-invalid event → quarantined raw; repaired version chained with reference to raw
    ├── Duplicate audit_id → chained once
    ├── Burst of 10k actions for one tenant → business throughput unaffected; chainer catches up
    ├── Two chainers race → fencing prevents fork
    ├── Records with tricky field values ("ab"/"c" vs "a"/"bc") → different hashes (length-prefixed)
    ├── Delete latest rows (staging) → truncation detected against anchor
    ├── Rewrite table and recompute hashes without key → verification fails
    ├── Hash mismatch → segment sealed, new segment continues, incident opened; auditing never stops
    ├── WORM export failure → alert; unanchored/unarchived window visible
    ├── Subject erasure → payload unreadable, verification still VALID
    └── Auditor query → recorded in platform chain; masked by default
```

[Certain] The core corrections follow from audit-integrity fundamentals:
- Defaulted actors falsify attribution.
- Chaining inside business transactions serializes them, while chaining separately loses atomicity.
- Clock-plus-counter IDs aren't monotonic.
- Unprefixed concatenation is ambiguous, and unkeyed in-database hashes are recomputable.
- Locking a ledger on mismatch disables auditing.
- Dropping invalid records destroys evidence.

[Likely] Checkpoint intervals and chain sharding thresholds are defaults.

[Guessing] Fail-closed scope per action class, pseudonymization of actors, and retention periods are compliance and legal decisions.

Two decisions are yours:
1. **Replace the "SYSTEM_KERNEL / GLOBAL_SYSTEM" default with quarantine, and require producers to declare system actors explicitly (AT-02).** Otherwise the audit trail attributes your bugs to the system.
2. **Move chaining out of the business transaction into a single chainer per chain, with keyed hashes and signed external checkpoints (AT-04 to AT-06).** That gives you throughput, ordering, and tamper evidence that an insider can't recompute away.
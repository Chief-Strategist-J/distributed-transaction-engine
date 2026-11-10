Your reconciliation starts from the engine ("sagas completed within window") and looks outward. It **cannot find the money problems that have no completed saga behind them**:
- **Orphan external charges.** The provider captured, but the saga crashed, failed, or never recorded the reference.
- **Duplicate charges.** The provider shows two captures for one saga.
- **Sagas stuck for days** in RUNNING or UNKNOWN, which never reach "completed."
- **Sagas that ended FAILED** with an executed external effect.

These are the anomalies that cost the most, and a one-directional scan never sees them. Reconciliation must also run **from the external side inward** (provider settlement reports matched against engine records), and it must cover non-terminal sagas. [Certain]

Four more errors, ranked by damage:

**1. A fixed 25-hour window ignores settlement timing, so "phantom settlement" fires on payments that simply haven't settled yet.** [Certain]
I disagree because banks and gateways settle T+1 or T+2. Refunds can sit in PENDING for days, and chargebacks arrive weeks later. NOT_FOUND at hour 2 usually means "not yet," not "failed." Auto-correcting on that reading creates the discrepancy you were trying to fix.
Here's what I'd do instead: classify against **expected settlement windows per provider and transaction type**. Never act on absence until the settlement window has passed **and** the provider's report for that period is confirmed complete.

**2. Auto-resolve acts on a single observation.** [Certain]
I disagree because a "ghost transaction" (external committed, engine compensated) is very often a refund **already in flight**: the compensation was sent and is pending at the bank. Issuing a "delayed refund" then refunds twice.
Here's what I'd do instead: auto-correction requires a stable anomaly (seen again after a delay), no in-flight compensation for that saga, and dispatch **through the compensation engine** with a deterministic key derived from saga plus anomaly type, so repeated runs can't double-act.

**3. Locking a tenant account automatically on drift above $10,000 punishes customers for your bugs.** [Certain]
I disagree because most large drifts come from settlement delays, report gaps, or your own defects, not customer fraud. Auto-locking turns a reconciliation lag into a customer outage.
Here's what I'd do instead: tiered risk actions (holding payouts is the usual first step) decided by **business policy with human approval**, and suppressed entirely when the anomaly pattern looks systemic (many tenants at once).

**4. The Redis leader lock lets runs overlap, and runs aren't idempotent.** [Certain]
I disagree because a run longer than 30 minutes, or a Redis failover, lets a second job start. With hourly runs over a 24-hour window, each saga is examined about 24 times. Without idempotent anomaly records and deterministic correction keys, one discrepancy becomes 24 anomalies and possibly 24 corrections.

The `$0.01` drift rule will also flood you. Fees, FX conversion, and rounding legitimately differ. Drift must be measured **against expected-difference rules** (fee schedules, FX tolerance), not against zero.

```
RECONCILIATION ENGINE — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (verify per provider)   [G] Guessing (finance/business must decide)
│
├── 0. SCOPE & SOURCES
│   ├── Internal sources: saga engine (sagas, steps, outcomes, provider references), internal ledger, inventory records
│   ├── External sources: provider settlement reports/files (authoritative for money), provider APIs (per-item lookup),
│   │   bank statements, supplier booking reports
│   ├── Directions (both mandatory)
│   │   ├── INTERNAL → EXTERNAL: every internal financial effect has its external counterpart
│   │   └── EXTERNAL → INTERNAL: every external movement has an internal owner (saga/ledger entry)
│   └── Run types
│       ├── Operational (intraday): catch stuck sagas, missing confirmations, duplicate charges early
│       ├── Settlement (daily, per provider report availability): money-level matching
│       └── Period close (monthly): finance sign-off, immutable results
│
├── 1. INVARIANTS
│   ├── RC-01 Reconciliation is two-directional (internal→external and external→internal) [C]
│   ├── RC-02 Absence is never acted on before the provider's settlement window passed AND the source is complete [C]
│   ├── RC-03 Every automated correction goes through the saga/compensation engine with a deterministic key
│   │        (saga id + anomaly type + target) — repeat runs cannot double-act [C]
│   ├── RC-04 An anomaly must be observed as stable (re-confirmed after a delay) before automated action [C]
│   ├── RC-05 Runs are idempotent: same input → same anomalies (unique per subject + class + period) [C]
│   ├── RC-06 Expected differences (fees, FX, rounding, partials) are rules, not anomalies [C]
│   ├── RC-07 Customer-impacting risk actions require business policy and human approval [C]
│   ├── RC-08 Systemic anomaly spikes stop automated corrections [C]
│   └── RC-09 Every anomaly ends with a recorded disposition [C]
│
├── 2. RUN CONTROL
│   ├── Job lease held in the database (holder, epoch, expiry, renewals); writes fenced by epoch
│   ├── Checkpoints per source and page/partition; resumable after crash
│   ├── Overlapping windows allowed; correctness from idempotent anomaly records (RC-05)
│   ├── Run status: COMPLETE | INCOMPLETE (source missing) | PARTIAL (some providers) | FAILED
│   └── Edge cases
│       ├── E2.1 Lock TTL shorter than run — second run overlaps — duplicate anomalies/corrections [C]
│       └── E2.2 Run crashes midway, restarts from scratch — re-calls provider APIs at scale — rate limits [C]
│
├── 3. SOURCE ACQUISITION & COMPLETENESS
│   ├── Provider reports
│   │   ├── Expected report per provider per day (availability time per provider) [G]
│   │   ├── Completeness checks: record count, control totals, checksum/sequence from provider
│   │   ├── Late/missing report → source INCOMPLETE for that period → no absence-based anomalies
│   │   └── Revised reports (provider re-issues) → re-run that period; anomalies updated, not duplicated
│   ├── Provider APIs
│   │   ├── Used for targeted lookups of unmatched items only (not per-saga sweeps) [L]
│   │   ├── Full pagination verified; rate-limited; responses cached per run
│   │   └── Eventual consistency: recent items may not appear yet
│   ├── Internal data
│   │   ├── Read from primary or replica with verified freshness (replica lag → stale internal view)
│   │   └── Snapshot time recorded per run
│   └── Edge cases
│       ├── E3.1 Missing daily report treated as "no transactions" — thousands of false MISSING_EXTERNAL [C]
│       ├── E3.2 API pagination bug drops last page — false anomalies for those items [C]
│       └── E3.3 Replica lag — recent saga outcomes missing — false GHOST anomalies [C]
│
├── 4. POPULATION SELECTION
│   ├── Internal side
│   │   ├── Terminal sagas (COMPLETED, COMPENSATED, FAILED, written-off) with financial steps in period
│   │   ├── Non-terminal sagas older than their expected duration (stuck/unknown) — operational runs
│   │   └── Selection by business date (value date/provider date) as well as engine timestamps
│   ├── External side
│   │   └── All movements in provider report for the period (captures, refunds, fees, chargebacks, payouts)
│   ├── Grace: items whose sagas are still within active processing (compensating, retrying) are tagged IN_FLIGHT
│   └── Edge cases
│       ├── E4.1 Selection by engine completion time only — provider value date in next period — mismatched periods [C]
│       ├── E4.2 Timezone cutoffs differ (provider UTC vs local business day) — items fall in wrong window [C]
│       └── E4.3 FAILED sagas excluded — external effect from failed step never found — LOSS [C]
│
├── 5. MATCHING
│   ├── Keys (priority order)
│   │   ├── Provider reference stored on the step outcome
│   │   ├── Idempotency-derived reference sent to provider (deterministic)
│   │   └── Fallback heuristics (amount + date + customer token) → candidate matches only (never auto-act)
│   ├── Cardinality
│   │   ├── One-to-one (capture ↔ payment step)
│   │   ├── One-to-many (one saga → partial captures / multiple refunds)
│   │   ├── Many-to-one (batched payouts, net settlements)
│   │   └── Aggregation by saga and by settlement batch before comparison
│   ├── Normalization: amounts in minor units, currency, sign conventions, fee vs gross/net
│   ├── Expected differences (RC-06) [G: finance-defined]
│   │   ├── Fee schedules per provider/product
│   │   ├── FX tolerance band per currency pair and rate source
│   │   ├── Rounding rules per currency
│   │   └── Partial capture/refund rules
│   └── Edge cases
│       ├── E5.1 Net settlement compared to gross saga amounts — every item mismatched [C]
│       ├── E5.2 Heuristic match treated as certain — wrong saga "balanced", real anomaly hidden [C]
│       └── E5.3 Multiple refunds for one saga compared individually — false mismatches [C]
│
├── 6. CLASSIFICATION  (engine state × external state, after matching)
│   ├── BALANCED — amounts within expected-difference rules, states agree
│   ├── TIMING — external not yet settled / pending; within settlement window → re-check later (no action)
│   ├── IN_FLIGHT — internal process still acting (compensation pending, retry scheduled) → re-check later
│   ├── GHOST_EXTERNAL — external committed, internal COMPENSATED/FAILED with no in-flight reversal
│   ├── MISSING_EXTERNAL — internal says executed, external absent after settlement window and complete source
│   ├── ORPHAN_EXTERNAL — external movement with no internal owner
│   ├── DUPLICATE_EXTERNAL — more external movements than internal intents for the same reference
│   ├── AMOUNT_MISMATCH — difference outside expected rules
│   ├── STATUS_MISMATCH — external reversed/charged back, internal still COMPLETED (or vice versa)
│   ├── STUCK_INTERNAL — saga non-terminal beyond expected duration
│   ├── INTERNAL_INCONSISTENCY — saga outcome disagrees with internal ledger
│   └── Edge cases
│       ├── E6.1 Pending refund classified as GHOST — auto refund issued — double refund — LOSS [C]
│       ├── E6.2 Chargeback months later not linked to saga — revenue overstated [C]
│       └── E6.3 Provider duplicate capture not detected (only checking "record exists") — customer double charged [C]
│
├── 7. STABILITY & VERIFICATION BEFORE ACTION  (RC-04)
│   ├── Re-observe after delay [L: next run or ≥ 1 hour] — anomaly persists with same values
│   ├── Confirm no in-flight compensation/saga activity for the subject
│   ├── Confirm source completeness for the period
│   ├── For MISSING_EXTERNAL: targeted API lookup by reference; provider support confirmation for high amounts
│   └── E7.1 Act on first observation during provider reporting lag — corrections that create the real discrepancy [C]
│
├── 8. RESOLUTION ACTIONS
│   ├── Auto-eligible (policy-configured, under amount caps) [G: caps]
│   │   ├── GHOST_EXTERNAL → trigger compensation via compensation engine (deterministic key, RC-03)
│   │   ├── STUCK_INTERNAL → trigger saga resolution (status queries, unknown-outcome ladder)
│   │   ├── DUPLICATE_EXTERNAL → refund the duplicate via compensation engine (key per duplicate reference)
│   │   ├── Missing webhook/notification → re-request provider status / resend internal notification
│   │   └── Each auto action: rate cap, amount cap, re-check before dispatch, outcome recorded on anomaly
│   ├── Manual (always)
│   │   ├── MISSING_EXTERNAL (engine believed success, money absent) — investigate before charging again
│   │   ├── ORPHAN_EXTERNAL — find owner; refund or attach to saga with evidence
│   │   ├── AMOUNT_MISMATCH beyond tolerance
│   │   ├── STATUS_MISMATCH (chargebacks, reversals) — business process
│   │   └── INTERNAL_INCONSISTENCY — engineering incident
│   └── Edge cases
│       ├── E8.1 Auto "re-charge" for MISSING_EXTERNAL — provider actually charged under another reference — double charge [C]
│       ├── E8.2 Correction dispatched directly to provider API, bypassing engine — no saga record, next run sees new orphan [C]
│       └── E8.3 Repeated runs dispatch correction again — prevented by deterministic key + anomaly state [C]
│
├── 9. RISK ACTIONS  (RC-07)
│   ├── Tiered: flag for review → hold payouts → restrict new high-value transactions → account restriction
│   ├── Triggered by business rules on stable, non-systemic anomalies only
│   ├── Human approval for any customer-facing restriction; customer communication defined [G]
│   └── E9.1 Automatic account lock from provider report delay — customer outage caused by own pipeline [C]
│
├── 10. ANOMALY LIFECYCLE
│   ├── Unique per (subject, class, period) — re-detection updates the same anomaly (RC-05)
│   ├── States: OPEN → (STABLE) → ACTION_IN_PROGRESS → RESOLVED (AUTO | MANUAL) | EXPECTED (timing confirmed) | WRITTEN_OFF
│   ├── Auto-close when later runs find a match (e.g., late settlement) — recorded, not deleted
│   ├── Aging SLAs per class and amount; escalation ladder
│   └── E10.1 Terminal "quarantined for audit" with no owner — anomalies age past dispute windows [C]
│
├── 11. SYSTEMIC DETECTION  (RC-08)
│   ├── Spike rules: anomaly count/amount far above baseline, or concentrated on one provider/report/day
│   ├── Action: stop auto-corrections and risk actions for affected scope; one incident; suspect source issues first
│   └── E11.1 Provider report format change — every item mismatched — mass auto-refunds — LOSS [C]
│
├── 12. REPORTING & PERIOD CLOSE
│   ├── Daily: matched totals, open anomalies by class/amount/age, auto-actions taken
│   ├── Period close: snapshot of balances and anomalies; finance sign-off; results immutable after close
│   ├── Late findings after close → adjustments in current period (never silent edits to closed periods)
│   └── E12.1 Closed period numbers change after late corrections — audit failure [C]
│
└── 13. FEATURE TEST SCENARIOS
    ├── Provider charged, saga crashed before recording → ORPHAN_EXTERNAL found from provider side
    ├── Provider double capture → DUPLICATE_EXTERNAL → refund of duplicate via engine
    ├── Saga completed, provider not yet settled (T+2) → TIMING, no action until window passes
    ├── Daily report missing → run INCOMPLETE; no MISSING_EXTERNAL anomalies created
    ├── Compensation refund pending at bank → IN_FLIGHT, not GHOST
    ├── GHOST stable across two runs, no in-flight reversal → auto compensation with deterministic key; repeat runs no duplicate
    ├── Fee difference within schedule → BALANCED
    ├── FX difference outside tolerance → AMOUNT_MISMATCH → manual
    ├── Chargeback weeks later → STATUS_MISMATCH linked to saga
    ├── Saga RUNNING for 3 days → STUCK_INTERNAL → resolution triggered
    ├── Two overlapping runs → same anomalies, no duplicate actions
    ├── Provider report format change → systemic detection stops auto-actions
    ├── Late settlement after anomaly opened → auto-closed as EXPECTED
    └── Period close → later adjustments booked in current period only
```

[Certain] The core corrections follow from how financial systems settle and report:
- Settlement and refund timing spans days.
- Missing reports look identical to missing transactions.
- One-directional scans miss orphan and duplicate external movements.
- Single observations during reporting lag produce false anomalies.
- Repeated runs need idempotent anomalies and deterministic correction keys.

[Likely] Provider report formats, completeness signals, and API consistency differ per provider.

[Guessing] Settlement windows, expected-difference rules, auto-correction caps, risk-action tiers, and customer communication are finance and business decisions.

Two decisions are yours:
1. **Add the external-to-internal direction, driven by provider settlement reports (§0, §4).** Without it, orphan and duplicate charges, the most expensive anomaly classes, are invisible by construction.
2. **Have finance define, per provider, the settlement windows and expected-difference rules (§5, §6).** Those two inputs decide whether this engine produces accurate anomalies or a flood of false ones that auto-correct money in the wrong direction.
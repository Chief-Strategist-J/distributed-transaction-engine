A list of "all" edge cases doesn't exist. What I can give you is the set of failures that v2 doesn't cover and that actually cause production incidents with versioned workflows.

The biggest gap: **anything a step reads live, outside the bundle, silently un-pins the saga**. Feature flags, fee tables, limits, and provider endpoints can all change underneath a "pinned" v2.4 saga. Pinning protects only what's inside the bundle. [Certain]

Below is an addendum to Workflow Versioning v2, numbered to continue it. Every node follows the format Scenario → Impact → Handling.

```
WORKFLOW VERSIONING — v2.1 ADDENDUM: CRITICAL EDGE & FAILURE CASES
Legend: [C] Certain   [L] Likely (verify per stack)   [G] Guessing (owner must decide)
│
├── 12. ADDITIONAL INVARIANTS
│   ├── WV-10 Everything that changes step behaviour is either inside the bundle or snapshotted at saga start [C]
│   ├── WV-11 Version resolution is part of the idempotent saga start (same key → same version, always) [C]
│   ├── WV-12 A bundle is loadable in every region before any saga in that region can resolve to it [C]
│   ├── WV-13 Bundles are integrity-checked (content hash) and signature-verified at load [C]
│   ├── WV-14 Each step attempt records the actual handler build and contract version that executed it [C]
│   ├── WV-15 Publish, promote, block, and migrate actions require authorization and a second approver [C]
│   └── WV-16 Orchestrator instances only own sagas whose bundles they can interpret (capability-aware ownership) [C]
│
├── 13. RESOLUTION & CREATION
│   ├── E13.1 Saga start retried after ambiguous failure; rollout changed between attempts — second attempt
│   │         resolves to a different version — duplicate sagas on two versions [C]
│   │   └── Handling: resolved version stored with the idempotency record (WV-11); retry returns original saga
│   ├── E13.2 Registry unavailable at saga creation [C]
│   │   ├── Fail closed (no new sagas) OR use last-known-good registry snapshot with max staleness [G]
│   │   └── Never fall back to "whatever bundle is in local cache" without status check
│   ├── E13.3 Version BLOCKED between resolution and creation commit — saga created on a blocked version [C]
│   │   └── Handling: version status re-checked inside the creation transaction
│   ├── E13.4 Tenant contractually pinned to a version that becomes BLOCKED — tenant's new sagas have nowhere to go [C]
│   │   └── Handling: tenant pin policy defines fallback version or rejection; tenant notified [G]
│   ├── E13.5 Related sagas for one business entity resolve to different versions
│   │         (order saga on v2, amendment saga on v3 with incompatible assumptions) — CORRUPT [C]
│   │   └── Handling: version stickiness per business entity for related workflows, or declared cross-version compatibility
│   └── E13.6 Clock/time-based activation ("activate at 00:00") evaluated by instance clocks — mixed versions around boundary [C]
│       └── Handling: activation by registry state change, not by instance clocks
│
├── 14. ROLLOUT & PROMOTION
│   ├── E14.1 Two operators promote different versions to ACTIVE simultaneously — two "latest" versions [C]
│   │   └── Handling: conditional update on registry; exactly one ACTIVE per (workflow, cohort)
│   ├── E14.2 Canary percentage increased; deterministic cohort boundary moves — entities flip between versions [C]
│   │   └── Handling: cohort assignment recorded per entity once assigned (sticky), not recomputed
│   ├── E14.3 Canary passes on low-risk traffic only (no high-value sagas in cohort) — defect hits first big payment on ACTIVE [C]
│   │   └── Handling: canary cohorts stratified by saga value/tenant tier [G]
│   ├── E14.4 Canary defect only appears in compensation path (rarely exercised) — promoted, then mass compensation failures [C]
│   │   └── Handling: compensation paths exercised by synthetic sagas before promotion (§26)
│   ├── E14.5 Rollback of ACTIVE → previous while previous is DEPRECATED (workers removed support) — nowhere to roll back [C]
│   │   └── Handling: previous ACTIVE stays fully supported until new version stable for a defined period [G]
│   └── E14.6 Promotion during incident freeze — change collides with incident response [L]
│       └── Handling: promotions blocked during declared freezes
│
├── 15. MULTI-REGION & DR
│   ├── E15.1 Version ACTIVE in region A, bundle not yet replicated to region B — sagas created in B fail to load [C]
│   │   └── Handling: WV-12 — activation per region only after bundle presence and integrity verified there
│   ├── E15.2 Failover mid-rollout: saga created in A on v3 resumes in B where v3 is not ACTIVE [C]
│   │   └── Handling: execution needs only bundle presence (not ACTIVE status) — bundles replicated before any activation
│   ├── E15.3 Registry state replicated asynchronously — region B still resolves to a version BLOCKED in A [C]
│   │   └── Handling: BLOCK propagates with priority; resolution checks local registry freshness bound
│   ├── E15.4 Worker in region B older than in region A — manifest contract unsupported in B [C]
│   │   └── Handling: worker support registry per region; WV-04 evaluated per region
│   └── E15.5 Regional data residency requires region-specific workflow variant (extra compliance step) [G]
│       └── Handling: variants are distinct versions; resolution policy selects by region
│
├── 16. BUNDLE INTEGRITY & LOADING
│   ├── E16.1 Bundle content hash mismatch (corruption, partial write, tampering) [C]
│   │   └── Handling: refuse execution; saga PAUSED; security/integrity incident (WV-13)
│   ├── E16.2 Bundle store unavailable — running sagas can't load definitions — engine-wide stall [C]
│   │   └── Handling: local immutable bundle cache keyed by (version, hash); cache populated before activation
│   ├── E16.3 Archived bundle stored only in cold storage — slow load during urgent compensation reopen [C]
│   │   └── Handling: definitions stay in primary database; archive is a copy
│   └── E16.4 Bundle references external schema by "latest" instead of pinned schema id — behaviour drifts [C]
│       └── Handling: all schemas referenced by exact id in bundle
│
├── 17. ORCHESTRATOR FLEET, DEPLOY & ROLLBACK
│   ├── E17.1 Mixed orchestrator fleet: older instance claims saga whose bundle needs a newer interpreter capability
│   │         — misinterpretation or crash loop [C]
│   │   └── Handling: WV-16 — ownership assignment filtered by capability; version activation requires full fleet capability
│   ├── E17.2 Orchestrator rollback after new state names/fields persisted — old code can't read them — sagas stuck [C]
│   │   └── Handling: expand/contract for persisted state; new states written only after fleet fully upgraded;
│   │                 rollback blocked once new states exist (or old code tolerant of unknown states → pause saga)
│   ├── E17.3 Interpreter bug fixed in orchestrator changes how an old bundle executes — pinned sagas change behaviour [C]
│   │   └── Handling: interpreter behaviour versioned by capability level; fixes affecting semantics are opt-in per bundle
│   └── E17.4 Orchestrator deploy changes idempotency key derivation code — retries of in-flight steps get new keys — duplicates [C]
│       └── Handling: key derivation algorithm versioned and pinned per saga
│
├── 18. WORKER & CONTRACT BEHAVIOUR DRIFT
│   ├── E18.1 Worker changes semantics while keeping the same contract version — pinned sagas see new behaviour [C]
│   │   └── Handling: semantic change = new contract version; contract tests per version in worker CI
│   ├── E18.2 Worker canary: two worker builds serve the same contract with different behaviour — nondeterministic outcomes [C]
│   │   └── Handling: WV-14 records build per attempt; canary limited to behaviour-preserving changes
│   ├── E18.3 Worker replies with a newer reply contract than manifest expects — parse failure — step UNKNOWN [C]
│   │   └── Handling: workers reply in the contract version of the command they received
│   ├── E18.4 Worker deploy gate checked in one region/environment only — handler removed elsewhere [C]
│   │   └── Handling: gate evaluated against every region's non-archived manifests
│   └── E18.5 Third-party provider changes API behaviour (not under your versioning) — pinned sagas behave differently [C]
│       └── Handling: provider adapters version their behaviour; provider changes create a new contract version
│
├── 19. LIVE INPUTS OUTSIDE THE BUNDLE  (WV-10)
│   ├── E19.1 Feature flag read at step time flips mid-saga — first steps under old logic, later under new — CORRUPT [C]
│   │   └── Handling: flags affecting workflow logic snapshotted at saga start (stored on saga) or part of bundle
│   ├── E19.2 Fee table/limits read live — compensation computes refund with new fees, not the ones charged — CORRUPT financial [C]
│   │   └── Handling: forward step outputs record values used; compensation uses recorded values
│   ├── E19.3 Endpoint/URL configuration changed — in-flight steps sent to new service version with different contract [C]
│   │   └── Handling: routing by contract version, not by mutable endpoint config
│   └── E19.4 Environment variable change on orchestrator alters timeouts for all versions — pinned policies overridden [C]
│       └── Handling: policies live in bundle; environment provides only infrastructure settings
│
├── 20. CHILD / SUB-WORKFLOWS
│   ├── E20.1 Parent pinned to v2 starts child with "latest" child version whose output schema changed — parent can't bind outputs [C]
│   │   └── Handling: parent manifest declares child version range; child start validated against parent's expected contract
│   ├── E20.2 Child version BLOCKED while parent waits to start it — parent stuck [C]
│   │   └── Handling: parent manifest lists fallback child version or parent pauses with clear reason
│   ├── E20.3 Parent migrated, children not (or reverse) — contract mismatch on child completion reply [C]
│   │   └── Handling: migration plan includes parent–child pairs; eligibility requires compatible pair
│   └── E20.4 Child compensation definitions retired before parent's compensation window closes [C]
│       └── Handling: child version archival gated on parent versions referencing it
│
├── 21. COMPENSATION-SPECIFIC VERSIONING
│   ├── E21.1 External provider sunsets the refund endpoint used by old version's compensation — old sagas can't compensate [C]
│   │   └── Handling: compensation-only migration (forward definition unchanged, compensation adapter upgraded) with own approvals
│   ├── E21.2 Compensation logic fixed in new version; old in-flight sagas keep buggy compensation [C]
│   │   └── Handling: compensation-only migration allowed at compensation safe points (no compensation in flight)
│   ├── E21.3 Compensation reopened by late reply years later (disputes) — old definition and provider contract gone [C]
│   │   └── Handling: reopen path uses archived definition + current adapter for that provider capability; manual process fallback [G]
│   └── E21.4 New version changes which step is the pivot — old sagas' compensation boundaries misjudged by new code [C]
│       └── Handling: pivot location read from pinned bundle only, never from current version
│
├── 22. MIGRATION — ADDITIONAL FAILURE CASES
│   ├── E22.1 Bulk migration partially fails midway — mixed versions without record of which succeeded [C]
│   │   └── Handling: migration id per saga, idempotent; progress table; resumable
│   ├── E22.2 Migration races with normal processing (reply arrives during migration) — CAS fails — retried at next safe point [C]
│   ├── E22.3 Migration of saga with pending timers — timer definitions (deadlines) differ in new version [C]
│   │   └── Handling: timers recomputed in migration transaction per new version rules (or kept, by explicit mapping)
│   ├── E22.4 Migrated saga has DLQ record / intervention referencing old version — operator acts with wrong definition [C]
│   │   └── Handling: migration blocked for sagas with open DLQ records/interventions, or those records updated atomically
│   ├── E22.5 Migration chain (v2.4 → v2.5 → v3) applied as direct jump without intermediate mappings — invalid state [C]
│   │   └── Handling: mappings composed and validated end-to-end before execution
│   ├── E22.6 Migration rollback after saga executed steps that exist only in new version — cannot roll back [C]
│   │   └── Handling: rollback allowed only before any new-version-only step executed
│   ├── E22.7 Migration load spike (thousands of sagas re-evaluated at once) — orchestrator/DB saturation [C]
│   │   └── Handling: rate-limited migration waves
│   └── E22.8 Migration changes customer-visible terms (price, delivery promise) — needs customer consent [G]
│       └── Handling: migrations classified SEMANTIC require business approval and customer communication
│
├── 23. LONG-RUNNING WORKFLOWS & CONTINUE-AS-NEW
│   ├── E23.1 Continue-as-new carries incomplete state (missed accumulated balance or schedule position) — CORRUPT [C]
│   │   └── Handling: explicit state transfer contract per workflow; validated by reconciliation after transfer
│   ├── E23.2 Old and new instances both active after crash during continue-as-new — duplicate billing [C]
│   │   └── Handling: close old + create new in one transaction; new instance keyed by (entity, checkpoint)
│   ├── E23.3 Checkpoint never reached (subscription paused indefinitely) — old version never drains [C]
│   │   └── Handling: forced checkpoint via migration at safe point, or support cost accepted explicitly
│   └── E23.4 History queries across instances broken (customer timeline split) — support confusion [L]
│       └── Handling: instances linked by lineage id
│
├── 24. ARCHIVE, AUDIT & EXPLAINABILITY
│   ├── E24.1 Regulator/dispute asks "what logic processed transaction X" — only workflow version known, not worker builds [C]
│   │   └── Handling: WV-14 — per-attempt handler build + contract version + bundle hash + flag snapshot recorded
│   ├── E24.2 Archived bundle unreadable by current tooling (format evolved) [C]
│   │   └── Handling: bundle format versioned; reader supports all archived formats
│   └── E24.3 Retention policy deletes old bundles while sagas referencing them are still within dispute windows [C]
│       └── Handling: bundle retention ≥ longest dispute/audit window of any saga that used it
│
├── 25. SECURITY OF VERSIONING  (WV-15)
│   ├── E25.1 Single engineer publishes and promotes a version that removes a fraud check — SECURITY [C]
│   │   └── Handling: publish and promote require separate approvers; diff of compliance-relevant steps highlighted
│   ├── E25.2 Registry write access compromised — malicious bundle injected [C]
│   │   └── Handling: signed bundles; signature verified at load; registry writes only via release pipeline
│   └── E25.3 Emergency hotfix bypasses validation "to save time" — broken version in production [C]
│       └── Handling: emergency path shortens approvals, never skips §3 validation
│
├── 26. VALIDATION BEFORE ACTIVATION
│   ├── Synthetic sagas exercising: happy path, every failure branch, every compensation, pivot boundary, parallel joins
│   ├── Replay of masked real saga shapes (inputs and step outputs) through new version in simulation mode
│   ├── Migration dry-run on a sample of real in-flight sagas: eligibility rate, mapping validity
│   └── E26.1 Version validated only on happy path — first rejection in production triggers untested compensation [C]
│
└── 27. ADDITIONAL TEST SCENARIOS
    ├── Retried saga start during rollout change → same saga, same version
    ├── Registry outage → creation behaves per policy; running sagas continue from local bundle cache
    ├── Simultaneous promotions → exactly one ACTIVE
    ├── Region failover mid-rollout → saga continues on its pinned version in other region
    ├── Corrupted bundle hash → saga paused, incident raised
    ├── Orchestrator instance lacking capability → never assigned sagas needing it
    ├── Orchestrator rollback with new persisted states → blocked or sagas paused safely
    ├── Feature flag flipped mid-saga → saga uses snapshot taken at start
    ├── Fee table change → compensation refunds recorded charged amount
    ├── Parent v2 with child output schema change in child "latest" → child start rejected or compatible version used
    ├── Provider refund endpoint sunset → compensation-only migration path works for old sagas
    ├── Bulk migration interrupted → resumes; no saga migrated twice
    ├── Continue-as-new crash → single active instance
    ├── Dispute after 2 years → full execution explanation available (bundle, builds, flags)
    └── Publish+promote by same person → blocked
```

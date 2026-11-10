A span for every function call won't give you end-to-end tracing. It gives you traces too large to store, too slow to render, too expensive to keep, and **still broken at the same async boundaries that are breaking them today**. Function-level spans don't fix a context that's lost at the outbox, in a goroutine started without `ctx`, or in a batch consumer.

I disagree with span-per-function because span creation, attributes, and export add real cost per call. In hot paths (serialization, loops, utility helpers called millions of times per second) that overhead dominates the work being measured, and trace backends cap spans per trace and trace size [Likely: Tempo has configurable per-trace limits]. You'd lose the long traces you care about most.

Here's what I'd do instead. Use three complementary OpenTelemetry-standard layers, connected so you can click from one to the next:
1. **Spans at every boundary and every business operation:** request, use case, DB adapter call, transaction, outbox, Kafka, RPC, cache, KMS, saga unit. Positive and negative paths are recorded as status, attributes, and span events.
2. **Continuous profiling, linked to spans:** this gives you the **full function call stack and utility-level detail** for every slow or failing span, without creating a span per function.
3. **On-demand deep tracing:** a time-boxed, scoped "verbose mode" that adds finer-grained spans for selected requests or tenants while you investigate.

The risk in insisting on span-per-function is that you pay the cost permanently and still don't get end-to-end flows.

Start with diagnosis (section 1 of the tree). In systems like yours, missing end-to-end traces are almost always propagation breaks, not missing instrumentation.

```
COMPLETE END-TO-END TRACING (OPENTELEMETRY) — DESIGN & DIAGNOSIS DECISION TREE
Legend: [C] Certain   [L] Likely (verify per SDK/agent/collector/backend version)   [G] Guessing (owner must decide)
│
├── 0. TARGET OUTCOME
│   ├── For any request/event: one connected view from edge → services → DB/cache/KMS → outbox → Kafka →
│   │   consumers → saga steps → compensations, for success AND failure paths
│   ├── For any slow/failed span: the function-level call stack (flame graph) of that exact span's execution
│   ├── For any flow: correlated logs and metrics (exemplars) one click away
│   └── Standards: W3C Trace Context (+ Baggage), OTLP for traces/metrics/logs/profiles, OTel semantic conventions
│
├── 1. WHY YOU DON'T SEE END-TO-END TODAY — DIAGNOSIS TREE  (walk in order)
│   ├── D1 Edge → service
│   │   ├── Envoy creates spans, but the application must forward incoming context to outgoing calls [C]
│   │   ├── Check: service server spans have Envoy span as parent?
│   │   └── Fix: instrumented server middleware (gRPC/HTTP) extracts context; propagator = W3C everywhere
│   ├── D2 Service → service
│   │   ├── Check: outbound HTTP/gRPC clients instrumented? (plain clients send no headers)
│   │   ├── Check: mixed propagators (B3 vs W3C) between services → trace splits [C]
│   │   └── Fix: instrumented clients; one propagator config (W3C trace context + baggage)
│   ├── D3 Inside a service (most common break)
│   │   ├── Go: goroutines started without passing ctx; ctx replaced by context.Background() in helpers [C]
│   │   ├── JVM/Kotlin: work submitted to executors without context wrapping; coroutines without context element [L]
│   │   ├── Worker pools/lanes reusing context from previous task (wrong parent) [C]
│   │   └── Fix: ctx/context passed explicitly to every function doing I/O; wrapped executors; lint rules
│   ├── D4 Service → database / cache / KMS
│   │   ├── Check: driver/adapter instrumented (pgx tracer, JDBC via agent, Redis, OpenSearch clients)?
│   │   └── Fix: instrumented adapters (§4.6)
│   ├── D5 Business transaction → outbox → relay → Kafka
│   │   ├── Context not stored in outbox row → relay has nothing to propagate [C]
│   │   ├── Relay injects its own context → consumer spans linked to relay, not to request [C]
│   │   └── Fix: capture at outbox insert; relay copies stored context to headers (§5)
│   ├── D6 Kafka → consumer
│   │   ├── Consumer instrumentation creates receive spans but processing runs in another goroutine/thread without context
│   │   ├── Batch processing: one span for batch, no per-record links
│   │   └── Fix: per-record process span with link to record context; batch span with links (§5)
│   ├── D7 Async saga continuations (timers, replies, compensations) start without stored context → orphan roots
│   │   └── Fix: store dispatch context on step/timer records; link on continuation (§5)
│   ├── D8 Sampling fragmentation
│   │   ├── Each hop samples independently → parts of the flow kept, others dropped [C]
│   │   ├── Tail sampling across multiple collector instances without trace-id routing → partial traces [C]
│   │   └── Fix: consistent sampling keyed on correlation/saga id; trace-id-aware load balancing (§7, §8)
│   ├── D9 Export losses
│   │   ├── SDK queue full / collector memory limiter refusing / backend per-trace limits [L]
│   │   └── Fix: drop counters visible; capacity; span budgets per trace (§11)
│   └── D10 Backend view
│       ├── Async parts are separate traces (by design) → view through links / correlation id search (TraceQL)
│       └── Fix: navigation by correlation id and saga id attributes (§9)
│
├── 2. SIGNALS & WHAT EACH IS FOR
│   ├── Traces (spans) — causality and latency across boundaries and business operations
│   ├── Span events — decision points and errors inside a span (cheap, ordered, timestamped)
│   ├── Span links — causality across async boundaries (outbox, Kafka, timers, compensations)
│   ├── Profiles (continuous profiling) — function-level call stacks: CPU, wall/off-CPU, allocations, locks, goroutines/threads
│   │   └── Linked to spans via span/trace id labels ("span profiles") [L: supported by Pyroscope for Go and Java]
│   ├── Logs — structured detail, correlated by trace id, span id, correlation id, event id
│   ├── Metrics — totals/rates/latency histograms (unsampled truth) with exemplars pointing to traces
│   └── Baggage — small allow-listed context (tenant tier, debug session id); never PII
│
├── 3. INSTRUMENTATION LEVELS  (decides what gets a span, an event, a metric, or only profiling)
│   ├── LEVEL A — ALWAYS SPAN (boundaries and I/O)
│   │   ├── Inbound requests/messages, outbound RPC/HTTP calls (per attempt), DB statements and transactions,
│   │   │   pool acquisition, cache calls, KMS/crypto service calls, object storage, Kafka produce/consume/process,
│   │   │   outbox insert and relay publish, saga units (transition, dispatch, reply, timer, compensation, intervention)
│   │   └── Rationale: these are where latency and failures live and where context must cross
│   ├── LEVEL B — SPAN FOR BUSINESS OPERATIONS (application/use-case layer)
│   │   ├── Use cases / application services (PlaceOrder, CapturePayment, ResolveUnknownOutcome)
│   │   ├── Domain services with meaningful work (pricing, eligibility, fraud scoring)
│   │   └── Rule: one span per operation, not per helper inside it
│   ├── LEVEL C — SPAN EVENTS (inside a span, no new span)
│   │   ├── Decisions: version guard outcome, idempotency hit, fencing rejection, retry scheduled, breaker open,
│   │   │   validation failure, business rejection reason, compensation chosen, cache hit/miss
│   │   └── Errors and exceptions (type, sanitized message, stack frames)
│   ├── LEVEL D — METRICS + PROFILING ONLY (no spans)
│   │   ├── Pure utilities and hot paths: serialization, mapping, hashing, validation helpers, small pure functions
│   │   ├── Measured by histograms/counters where useful, and by continuous profiling for call stacks
│   │   └── Promote to LEVEL B only if a utility does I/O, can block, or regularly exceeds a latency threshold [L: > 1 ms]
│   └── LEVEL E — ON-DEMAND DEEP MODE (time-boxed, scoped)
│       ├── Activated for: specific correlation id / saga id / tenant / route, with expiry
│       ├── Adds: spans for selected internal functions (decorated methods), richer events
│       ├── Propagated via baggage key (allow-listed) so every service honours it for that flow
│       └── Hard caps: max spans per trace, max duration, max sessions concurrently [G]
│
├── 4. INSTRUMENTATION BY LAYER  (positive and negative paths)
│   ├── 4.1 Edge (Envoy)
│   │   ├── Tracing provider configured with OTLP; W3C propagation; untrusted inbound context linked, not adopted
│   │   └── Attributes: route, method, response code, upstream cluster, response flags (proxy-generated errors)
│   ├── 4.2 Inbound handlers (gRPC/HTTP server middleware)
│   │   ├── Server span per request; status from response class; request id, route template (not raw path)
│   │   └── Negative: auth failures (401/403), validation (400/422), rate limits (429), panics recovered (500)
│   ├── 4.3 Application/use-case layer (LEVEL B)
│   │   ├── Span per use case; attributes: tenant (opaque), saga id, idempotency fingerprint (hashed), operation outcome
│   │   └── Negative: business rejection → status OK + outcome attribute + event; unexpected → status ERROR
│   ├── 4.4 Domain logic
│   │   ├── Decision events (LEVEL C); no spans per entity method
│   │   └── Panics/exceptions recorded on enclosing span with stack frames
│   ├── 4.5 Saga engine
│   │   ├── saga.transition (per transaction) → events: CAS result, epoch check, outbox rows written, timers set
│   │   ├── saga.step.dispatch → attributes: step, attempt, idempotency fingerprint, contract version
│   │   ├── saga.reply.process (link to stored dispatch context) → events: outcome class, late reply, duplicate
│   │   ├── saga.timer.fire (link) → events: obsolete timer, retry dispatched
│   │   ├── saga.compensation.* (links to failing step) → events: compensation outcome, status query result
│   │   └── saga.intervention (link) → operator action type, approval id (no personal data)
│   ├── 4.6 DB adapter layer (PostgreSQL)
│   │   ├── Spans
│   │   │   ├── db.pool.acquire (wait time; timeouts are a top hidden latency cause)
│   │   │   ├── db.transaction (begin → commit/rollback; outcome attribute; isolation level)
│   │   │   └── db statement spans (per query): operation, table/collection, sanitized statement, rows affected/returned
│   │   ├── Events inside statement/transaction spans
│   │   │   ├── CAS zero rows (with diagnosed cause), unique violation (conflict-ignore result), deadlock detected,
│   │   │   │   serialization failure, lock timeout, statement timeout, retry of transaction
│   │   ├── Instrumentation source: driver-level tracer (Go: pgx tracer integration [L]; JVM: JDBC via agent)
│   │   │   + repository-level span for each repository method (LEVEL B) when it bundles several statements
│   │   └── Negative paths: connection refused, pool exhausted, timeouts, constraint violations, rollback reasons
│   ├── 4.7 Cache / search / object storage / KMS adapters
│   │   ├── Client spans per call; cache hit/miss as attribute; bulk item failures as events (search)
│   │   └── KMS: decrypt outcome class (DECRYPTED/UNAVAILABLE/TAMPERED/ERASED) — never key material
│   ├── 4.8 Outbound RPC/HTTP clients
│   │   ├── Span per attempt (retries visible); attempt number; deadline remaining; breaker state
│   │   └── Negative: timeouts, resets, 5xx, proxy-generated errors, ambiguous outcomes (attribute)
│   ├── 4.9 Messaging
│   │   ├── Producer: publish span (relay) linking to stored outbox context
│   │   ├── Consumer: receive/process spans per record (or batch span + links); attributes: topic, partition, offset,
│   │   │   consumer group, event id, event type, decision outcome (APPLIED/DUPLICATE/STALE/GAP/HELD/QUARANTINED)
│   │   └── Negative: deserialization failure, upcast failure, quarantine, parked key, retry scheduled
│   └── 4.10 Utilities (LEVEL D unless promoted)
│       ├── Serialization, encryption library, upcasters, idempotency helpers, mappers → metrics + profiling
│       ├── Promote with a decorator only if I/O or regularly slow; never inside tight loops
│       └── Errors from utilities recorded on the caller's span (event with type and frames)
│
├── 5. ASYNC BOUNDARIES  (where end-to-end usually breaks)
│   ├── Outbox: store traceparent/tracestate in outbox row at insert (inside business tx span)
│   ├── Relay: publish span links to stored context; headers carry stored context (not relay's)
│   ├── Consumer: new trace (or child if lag < threshold [L]) with link to producer context; correlation id attribute
│   ├── Saga dispatch → reply: dispatch context stored on step attempt; reply span links to it
│   ├── Timers / scheduled jobs: context stored on timer/schedule occurrence; firing span links to it
│   ├── Compensations / redrives / interventions: link to originating step context
│   ├── Provider webhooks: new root; provider references as attributes; link resolved by saga id
│   └── Navigation: correlation id and saga id on every span → one query shows all linked traces of a flow
│
├── 6. NEGATIVE-PATH COVERAGE MATRIX  (how each failure appears)
│   ├── Validation error → server span 400/422, event "validation_failed" (field paths, no values), status OK or ERROR per policy [G]
│   ├── Authentication/authorization failure → 401/403 span, event reason class, status OK (expected) or ERROR (unexpected)
│   ├── Business rejection → status OK, attribute outcome=REJECTED, event with reason code
│   ├── Timeout / deadline exceeded → status ERROR, attribute ambiguous=true where outcome unknown
│   ├── Retry → each attempt span; parent records retry events and final outcome
│   ├── Panic / unhandled exception → status ERROR, exception event with stack frames; recovered at boundary
│   ├── Cancellation (context canceled by caller) → status per policy, attribute cancelled_by=caller/deadline
│   ├── DB conflict/deadlock/serialization failure → statement/transaction span events; retry visible
│   ├── Duplicate / stale / gap (consumers) → status OK, decision attribute, event
│   ├── Quarantine / DLQ / parked key → status ERROR (needs attention), event with class and fingerprint
│   ├── Compensation path → compensation spans linked; outcome attributes
│   └── Partial failure (bulk) → parent span with counts; per-item failures as events (capped)
│
├── 7. SAMPLING FOR COMPLETENESS
│   ├── Errors: keep 100% via tail sampling (status ERROR, NEEDS_ATTENTION, compensation, quarantine)
│   ├── Slow: keep above latency threshold per operation
│   ├── Flows: deterministic decision on correlation/saga id so all hops of a sampled flow are kept [L]
│   ├── Debug sessions (LEVEL E): forced keep for scoped flows, capped and expiring
│   ├── Base rate for healthy traffic per tier [G]
│   └── Untrusted inbound sampled flags ignored
│
├── 8. COLLECTOR PIPELINE
│   ├── Agent (per node): receive OTLP → memory limiter → redaction → batch → forward
│   ├── Gateway tier: trace-id-aware load balancing → span metrics (pre-sampling) → tail sampling → export
│   ├── Profiles pipeline: profiler agents (eBPF or language profilers) → profiles backend, labelled with service,
│   │   version, and span/trace ids where supported [L: OTel profiles signal maturing; Pyroscope span profiles available]
│   ├── Logs pipeline: trace/span ids attached by logging bridge
│   └── Backpressure: drops counted at each stage; never block applications
│
├── 9. BACKEND & NAVIGATION  (Grafana stack)
│   ├── Traces (Tempo): search by correlation id, saga id, tenant, error status; TraceQL queries across linked traces
│   ├── Trace → profile: open flame graph for a span's execution (span profiles) [L]
│   ├── Trace → logs: logs filtered by trace id / span id
│   ├── Metrics → trace: exemplars on latency histograms
│   ├── Service graph: generated from spans (pre-sampling metrics)
│   └── Saga timeline: from engine history + saga id (traces for detail per unit)
│
├── 10. RUNTIME SPECIFICS
│   ├── Go
│   │   ├── Pass ctx as first parameter to every function that does I/O or creates spans; lint for context.Background()
│   │   │   inside request paths
│   │   ├── Goroutines: pass ctx explicitly; errgroup with ctx; panics recovered at goroutine boundary and recorded
│   │   ├── Instrumentation: otelhttp, otelgrpc (or Connect interceptors), pgx tracer, Kafka client instrumentation [L]
│   │   ├── Profiling: continuous profiler with pprof labels carrying span id for span-profile linking [L]
│   │   └── eBPF auto-instrumentation available for some Go libraries without code changes [L: maturity varies]
│   ├── JVM / Kotlin
│   │   ├── OpenTelemetry Java agent auto-instruments JDBC, gRPC, HTTP clients/servers, Kafka clients [C]
│   │   ├── Coroutines: OTel context propagated via coroutine context integration [L]
│   │   ├── Executors: context-wrapped executors; Kafka Streams processing context [L]
│   │   ├── Manual spans via annotations/API for use cases (LEVEL B)
│   │   └── Profiling: async-profiler based continuous profiling with span profiles [L]
│   └── Kafka Connect / Debezium
│       └── Relay propagation via stored context columns placed into headers [L: router configuration]
│
├── 11. OVERHEAD & SAFETY BUDGETS
│   ├── Span budget per trace [L: e.g., ≤ 1,000 spans]; deep mode capped separately
│   ├── Attribute/event/link limits set explicitly in SDKs
│   ├── Profiling overhead target [L: low single-digit % CPU]; sampling profilers, not instrumenting profilers
│   ├── No spans inside tight loops; per-item work summarized on parent span
│   ├── Export non-blocking; bounded queues; drop counters alerting
│   └── E11.1 Span-per-function in serialization path — CPU regression and trace size limits hit — data dropped [C]
│
├── 12. DATA PROTECTION
│   ├── Redaction at SDK (attribute allow-lists) and collector (processors)
│   ├── Statements sanitized; no request/response bodies; no secrets/headers with credentials
│   ├── Stack frames (code locations) allowed; exception messages sanitized (may contain customer data)
│   ├── Profiles contain code locations only (no payload data) [C]
│   └── Baggage allow-list; stripped at egress to external parties
│
├── 13. VERIFICATION — "GOLDEN TRACES"
│   ├── For each critical flow, define expected span tree shape (positive and negative variants):
│   │   e.g., PlaceOrder success; payment timeout → UNKNOWN → status query; compensation; DLQ quarantine
│   ├── Integration tests export to an in-memory exporter and assert: parent/child/link relations, required attributes,
│   │   status codes, events present, no forbidden attributes
│   ├── Production canary flows always sampled; dashboard of propagation coverage per hop:
│   │   % consumer spans with producer link, % server spans with parent, orphan roots per service
│   └── Library/agent upgrades must pass golden-trace tests
│
└── 14. TEST SCENARIOS
    ├── HTTP request → Envoy → API → use case → DB transaction → outbox: single trace with all spans
    ├── Outbox relay → Kafka → consumer → saga transition: consumer trace linked to request trace via stored context
    ├── Goroutine/coroutine work inside request → child spans, never orphan roots
    ├── DB pool exhaustion → pool acquire span shows wait; timeout event; request span ERROR
    ├── Deadlock and transaction retry → events on transaction span; retry attempt visible
    ├── Payment timeout → client span ERROR ambiguous; saga UNKNOWN resolution spans linked
    ├── Business rejection → OK status with outcome attribute; not counted as error
    ├── Panic in handler → recovered; ERROR span with stack frames; no payload data
    ├── Slow use case → open span profile → flame graph shows utility function hotspots
    ├── Deep mode for one correlation id → extra spans across all services for that flow only; auto-expires
    ├── Error in unsampled flow → kept by tail sampling
    ├── Collector restart → no application impact; drop counters visible
    └── Golden trace tests fail when a library upgrade breaks propagation
```

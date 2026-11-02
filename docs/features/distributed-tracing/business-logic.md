# Distributed Tracing — Business Logic Decision Tree

```
DISTRIBUTED TRACING (BUSINESS DECISION TREE)
│
├── 1. CONTEXT EXTRACTION & PROPAGATION TIER
│   └── Trigger: Inbound Request Enters Engine Boundary (HTTP, gRPC, or Kafka Record)
│       ├── Check: Is W3C `traceparent` Header Present in Request Metadata?
│       │   ├── YES ──> Action: Parse TraceContext (TraceID, ParentSpanID, TraceFlags)
│       │   │   ├── Check: Is TraceID Valid 16-Byte Hex String?
│       │   │   │   ├── YES ──> Action: Adopt Inbound TraceID as Root / Parent
│       │   │   │   └── NO ──> Action: Discard Malformed Context & Generate New TraceID
│       │   └── NO ──> Action: Generate High-Entropy 128-Bit W3C TraceID
│       │
│       └── State: CONTEXT_INITIALIZED
│
├── 2. SAMPLING DECISION & SPAN LIFECYCLE TIER
│   └── State: CONTEXT_INITIALIZED
│       ├── Action: Evaluate Head-Based Adaptive Sampler:
│       │   ├── Check: Is Inbound Trace Flagged as Sampled (TraceFlags=01)?
│       │   │   ├── YES ──> Set Sampled = TRUE
│       │   │   └── NO ──> Check: Dynamic Tenant Sampling Rules:
│       │   │       ├── Condition: Error Rate Spike Detected OR High-Value VIP Tenant
│       │   │       │   └── Set Sampled = TRUE (100% Capture)
│       │   │       └── Condition: Standard Healthy Path
│       │   │           └── Apply Rate-Limiting Sampler (e.g. 10 traces/second per node)
│       │
│       └── Action: Begin Execution Span (e.g. `saga.step.execute`)
│           ├── Inject Span Attributes:
│           │   ├── `tenant.id`, `saga.id`, `step.name`, `step.idempotency_key`, `fencing.token`
│           │   └── OpenTelemetry Semantic Conventions for DB and RPC
│           │
│           └── Sub-Execution: Dispath Remote Sub-Requests (HTTP/gRPC/Kafka)
│               └── Action: Inject Current Span Context into Outbound Headers (`traceparent`, `tracestate`)
│
└── 3. SPAN CLOSURE & ASYNCHRONOUS EXPORT TIER
    └── Trigger: Saga Step / Transaction Execution Finishes (Return or Error)
        ├── Action: Attach Execution Outcome to Span:
        │   ├── If Exception / Failure: Set Status=ERROR, Record Error Event & Stacktrace
        │   └── If Success: Set Status=OK
        │
        └── Action: Flush Span to In-Memory Ring Buffer
            ├── Check: Is Buffer Full (Exceeds 10,000 Spans Queue)?
            │   ├── YES ──> Action: Increment `otel_dropped_spans_total` & Drop Tail (Prevent OOM)
            │   └── NO ──> Action: Asynchronously Batch Spans (Every 500ms or 512 Spans)
            │       └── Action: Export via OTLP/gRPC to OpenTelemetry Collector (:4317)
            │           ├── Collector Returns SUCCESS
            │           │   └── State: EXPORTED ──> [TERMINAL: Spans Stored in Grafana Tempo]
            │           └── Collector Returns FAILURE (Transient Network / Unavailable)
            │               └── Action: Retry with Exponential Backoff Buffer (Max 3 Attempts)
```

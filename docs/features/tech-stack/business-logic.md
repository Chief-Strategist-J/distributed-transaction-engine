# Technology Stack — Business Logic Decision Tree

```
TECHNOLOGY STACK ARCHITECTURE (DECISION TREE)
│
├── 1. WORKLOAD & LANGUAGE RUNTIME SELECTION
│   └── Trigger: Implementing Engine Core, Network Ingress, or High-Performance Compute
│       ├── Domain A: Orchestration Core & Distributed State Management
│       │   └── Choice: Golang (1.23+)
│       │       ├── Rationale: Native CSP goroutine concurrency, predictable GC pauses, rich cloud-native ecosystem
│       │       └── Enforcement: Strict linting via golangci-lint, explicit context.Context propagation
│       │
│       └── Domain B: High-Performance Crypto, Fencing Primitives & Ultra-Low-Latency Hashing
│           └── Choice: Rust (1.81+) via CGo or Native Sidecar
│               └── Rationale: Zero-cost abstractions, memory safety without GC, cryptographic determinism
│
├── 2. STATE STORAGE & MESSAGING TIER
│   ├── Component: Ground-Truth ACID Ledger & Relational State
│   │   └── Choice: PostgreSQL 18
│   │       ├── Usage: Saga instance tables, step state, transactional outbox, immutable audit log
│   │       └── Guarantee: Serializable / Read Committed Isolation, Foreign Key Integrity
│   │
│   ├── Component: High-Throughput Distributed Mutex & Ephemeral Cache
│   │   └── Choice: Redis Cluster 7.4 (Standalone in KinD dev)
│   │       ├── Usage: Idempotency deduplication, distributed lock leasing, rate limiting, bloom filters
│   │       └── Guarantee: Sub-millisecond lookup, atomic Lua script execution
│   │
│   └── Component: Distributed Event Bus & Outbox Streaming
│       └── Choice: Apache Kafka 3.8 (KRaft Mode, No Zookeeper)
│           ├── Usage: Saga event publishing, dead-letter queue, asynchronous step triggers
│           └── Guarantee: At-least-once partitioned streaming, consumer group horizontal scaling
│
└── 3. OBSERVABILITY & ORCHESTRATION PLATFORM
    ├── Platform: Container Orchestration & Service Management
    │   └── Choice: Kubernetes 1.30+ (KinD for Local Dev, EKS/GKE for Cloud)
    │       └── Guarantee: Self-healing pods, headless cluster services, dynamic config maps & secrets
    │
    └── Observability: LGTM Stack (Loki, Grafana, Tempo, Prometheus) via OpenTelemetry Collector
        ├── Metrics: Prometheus scraping `:8081/metrics`
        ├── Traces: OpenTelemetry OTLP/gRPC exporting to Tempo
        ├── Logs: Structured JSON written to stdout, collected by Promtail/OTel to Loki
        └── Visualization: Unified Grafana 11.2 Dashboards (:3000)
```

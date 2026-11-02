# Operational Dashboard — Business Logic Decision Tree

```
OPERATIONAL DASHBOARD (BUSINESS DECISION TREE)
│
├── 1. USER AUTHENTICATION & SSE STREAM SUBSCRIPTION TIER
│   └── Trigger: Operator Opens Browser & Connects to Dashboard Gateway
│       ├── Check: Validate Session Cookie / JWT via Gateway Auth
│       │   ├── Invalid / Expired ──> [Redirect to Login Portal / HTTP 401]
│       │   └── Valid ──> Action: Establish Server-Sent Events (SSE) or WebSocket Stream:
│       │       └── Route: `/api/v1/stream/sagas?tenant={id}&filter={status}`
│       │           └── State: CLIENT_STREAM_ACTIVE
│
├── 2. REAL-TIME TELEMETRY AGGREGATION TIER
│   └── State: CLIENT_STREAM_ACTIVE
│       ├── Background Process: Ingest Live System Metrics & Events
│       │   ├── Ingestion Source 1: Engine Prometheus Scrape Endpoint (:8081/metrics)
│       │   │   └── Pull: Active Sagas Gauge, Step Latency Histograms, Outbox Lag Gauge
│       │   ├── Ingestion Source 2: Kafka CDC / Engine State Transition Notification Topic
│       │   │   └── Pull: Live Saga State Transitions (CREATED -> RUNNING -> COMPLETED)
│       │   └── Ingestion Source 3: Dead-Letter Queue Counter
│       │
│       └── Action: Buffer and Throttle Pushes to Browser (Push Window: 500ms Throttle):
│           ├── Payload: Active Transactions Count, Failure Rate (%), DLQ Pending Count, P99 Latency
│           └── Client Action: Render Real-Time Canvas / React SVG Metric Nodes
│
├── 3. DRILL-DOWN & INTERACTIVE MANAGEMENT TIER
│   └── Trigger: Operator Clicks Specific Saga Node in Dashboard UI
│       ├── Action: Fetch Saga Execution Graph & Step Details via REST API:
│       │   ├── Query: `GET /api/v1/transactions/{id}`
│       │   ├── Check: Does Saga Exist in PostgreSQL / Redis?
│       │   │   ├── NO ──> Return 404 Not Found Modal
│       │   │   └── YES ──> Return Complete Step DAG, Logs, Timing, and Error Payloads
│       │   │
│       │   └── Rendering: Display Visual DAG Graph (Green: Succeeded, Blue: Running, Red: Failed)
│       │
│       └── Action: Operator Triggers Management Action (Pause, Redrive, Quarantine)
│           ├── Check: Does User Role Contain 'sre:admin'?
│           │   ├── NO ──> Disable Action Buttons / Show Read-Only Banner
│           │   └── YES ──> Open Confirmation Modal with Impact Analysis
│           │       └── On Confirm: POST `/api/v1/transactions/{id}/intervention`
│           │           └── Handover to Manual Intervention Business Logic
```

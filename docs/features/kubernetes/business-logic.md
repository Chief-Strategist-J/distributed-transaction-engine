# Kubernetes Deployment & Runtime — Business Logic Decision Tree

```
KUBERNETES RUNTIME & ORCHESTRATION (BUSINESS DECISION TREE)
│
├── 1. POD STARTUP & PROBE EVALUATION TIER
│   └── Trigger: Kubelet Schedules Engine Pod to Node
│       ├── Action: Container Engine Pulls Image & Launches Entrypoint
│       │
│       ├── Health Check Phase A: Startup Probe (`GET :8081/health/live`, failureThreshold=30)
│       │   ├── Check: Can Engine connect to PostgreSQL, Redis, and Kafka cluster?
│       │   │   ├── NO ──> Probe Returns HTTP 503 ──> [Kubelet Waits & Retries]
│       │   │   └── YES ──> Probe Returns HTTP 200 ──> Startup Succeeded! Handover to Liveness & Readiness
│       │
│       ├── Health Check Phase B: Liveness Probe (`GET :8081/health/live`, periodSeconds=10)
│       │   ├── Condition: Engine Goroutines Deadlocked OR Main Event Loop Frozen (>30s)
│       │   │   └── Probe Returns HTTP 500 / Times Out (3 Consecutive Failures)
│       │   │       └── Action: Kubelet Kills Container (SIGTERM -> SIGKILL) & Restarts Pod
│       │   └── Condition: Healthy Heartbeat
│       │       └── Probe Returns HTTP 200 ──> Continue Running
│       │
│       └── Health Check Phase C: Readiness Probe (`GET :8081/health/ready`, periodSeconds=5)
│           ├── Condition: PostgreSQL / Redis Latency Spike OR Worker Saturated (CPU > 95%)
│           │   └── Probe Returns HTTP 503
│           │       └── Action: Endpoints Controller Removes Pod IP from Service Target Slice (Stops Ingress Traffic)
│           └── Condition: Buffer Capacity Available & DB Accessible
│               └── Probe Returns HTTP 200
│                   └── Action: Pod IP Remains in Service Pool (Accepting Client & Worker Traffic)
│
├── 2. ELASTIC AUTOSCALING (HPA) TIER
│   └── Trigger: Metrics Server / Prometheus Adapter Reports Engine Load
│       ├── Action: Evaluate Target Metrics:
│       │   ├── Metric 1: Average Pod CPU Utilization (Target: 70%)
│       │   └── Metric 2: Kafka Consumer Group Lag (`kafka_consumer_group_lag > 500`)
│       ├── Evaluation:
│       │   ├── Condition: Load Exceeds Target Threshold AND Current Replicas < Max (10)
│       │   │   └── Action: Scale Out (Add Pods) ──> New Pods Pass Readiness & Absorb Partitions
│       │   ├── Condition: Load Below Scale-Down Threshold for Stabilization Window (300s)
│       │   │   └── Action: Scale In (Terminate Excess Pods Gracefully)
│       │   └── Condition: Metric within Normal Range
│       │       └── Action: Maintain Current Replica Count (Steady State)
│
└── 3. GRACEFUL TERMINATION & PRE-STOP HOOK TIER
    └── Trigger: Pod Termination Signal Issued (Node Drain / Deploy Rollout / Scale In)
        ├── Action: Kubelet Removes Pod IP from Service Endpoints & Sends SIGTERM
        ├── Action: Engine Traps SIGTERM and Enters DRAIN Mode:
        │   ├── Step 1: Reject New Inbound HTTP/gRPC Requests (Return HTTP 503 Service Shutting Down)
        │   ├── Step 2: Pause Kafka Consumer Listeners (Cease Consuming New Topic Offsets)
        │   ├── Step 3: Wait for In-Flight Saga Steps to Complete or Checkpoint (Grace Period: 30s)
        │   ├── Step 4: Flush Transactional Outbox Buffer and Disconnect DB Pools
        │   └── Step 5: Flush OpenTelemetry Trace Spans to Collector
        └── Action: Engine Process Exits Cleanly with Code 0
            └── State: TERMINATED ──> [TERMINAL: Zero In-Flight Data Corruption or Orphaned Transactions]
```

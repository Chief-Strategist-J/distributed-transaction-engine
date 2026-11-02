# Kubernetes Architecture, Network Topology & Component Connection Diagrams

## 1. End-to-End Ingress, NodePort & Service Discovery Architecture

This diagram details the connection logic from external clients down to individual pod network namespaces across KinD nodes.

```mermaid
graph TB
    subgraph Host Network
        Dev["Developer / Browser"]
        ClientApp["Upstream Microservices"]
    end

    subgraph KinD Control-Plane Node
        NP8080["NodePort 30080 (:8080)"]
        NP3000["NodePort 30000 (:3000)"]
        NP5432["NodePort 30432 (:5432)"]
        NP9092["NodePort 30092 (:9092)"]
        NP13133["NodePort 31133 (:13133)"]
    end

    subgraph KinD Worker Node Namespace: dist-tx
        subgraph Kubernetes Service Layer
            SvcIngress["svc/distributed-transaction-engine :8080"]
            SvcGrafana["svc/grafana :3000"]
            SvcPostgres["svc/postgres :5432"]
            SvcKafka["svc/kafka :9092"]
            SvcRedis["svc/redis :6379"]
            SvcOTel["svc/otel-collector :4317/:4318/:8889"]
            SvcProm["svc/prometheus :9090"]
            SvcTempo["svc/tempo :3200"]
            SvcLoki["svc/loki :3100"]
        end

        subgraph Pod Workloads
            PodEngine1["pod/distributed-transaction-engine-replica-1"]
            PodEngine2["pod/distributed-transaction-engine-replica-2"]
            PodGrafana["pod/grafana"]
            PodPostgres["pod/postgres-0 (StatefulSet)"]
            PodKafka["pod/kafka-0 (StatefulSet)"]
            PodRedis["pod/redis (Deployment)"]
            PodOTel["pod/otel-collector (Deployment)"]
            PodProm["pod/prometheus (Deployment)"]
            PodTempo["pod/tempo (Deployment)"]
            PodLoki["pod/loki (Deployment)"]
        end
    end

    Dev -->|HTTP :3000| NP3000 --> SvcGrafana --> PodGrafana
    Dev -->|SQL :5432| NP5432 --> SvcPostgres --> PodPostgres
    Dev -->|Events :9092| NP9092 --> SvcKafka --> PodKafka
    ClientApp -->|REST/gRPC :8080| NP8080 --> SvcIngress --> PodEngine1 & PodEngine2
    Dev -->|Health :13133| NP13133 --> SvcOTel --> PodOTel
```

---

## 2. In-Cluster Telemetry Pipeline & Cross-Service Correlation

This sequence details how logs, traces, and metrics flow across the OpenTelemetry Collector into Tempo, Loki, and Prometheus, and how Grafana correlates them.

```mermaid
sequenceDiagram
    autonumber
    participant App as Engine Pod
    participant OTel as svc/otel-collector:4318
    participant Tempo as svc/tempo:4317 (Traces)
    participant Loki as svc/loki:3100 (Logs)
    participant Prom as svc/prometheus:9090 (Metrics)
    participant Grafana as svc/grafana:3000 (UI)

    App->>OTel: POST /v1/traces (Span: "saga.commit", trace_id: "0xabc123")
    App->>OTel: POST /v1/logs ("Transaction committed", trace_id: "0xabc123")
    
    OTel->>Tempo: Forward trace span via OTLP gRPC (:4317)
    OTel->>Loki: Forward log line with labels {service.name, environment}
    
    loop Every 15 seconds
        Prom->>OTel: Scrape /metrics (:8889)
        OTel-->>Prom: Export otel_kafka_brokers, pg_stat, redis_memory
    end

    Note over Grafana: Operator investigates transaction incident
    Grafana->>Loki: Query logs {service="distributed-transaction-engine"}
    Loki-->>Grafana: Returns log line with clickable TraceID: 0xabc123
    Grafana->>Tempo: Follow trace link to Tempo span view
    Tempo-->>Grafana: Render complete waterfall flamegraph
    Grafana->>Prom: Render correlated CPU and Kafka offset charts
```

---

## 3. Component Relationship & Storage Dependency Matrix

This diagram maps how the polyglot engine interacts with the primary database, distributed cache, and event broker inside the cluster.

```mermaid
graph TD
    subgraph Transaction Engine Pod
        GoLayer["Go 1.23 DAG Scheduler"]
        RustLayer["Rust 1.81 State Machine & Outbox"]
        CppLayer["C++20 Rollback Kernel"]
    end

    subgraph State & Coordination
        Redis[("Redis 7.4 (Idempotency Keys & Lock Fencing)")]
        PG[("PostgreSQL 18 (ACID Ledger & Outbox Table)")]
        Kafka[("Apache Kafka 4.4 (KRaft Partitioned Event Bus)")]
    end

    GoLayer -->|1. Check / Acquire Lock| Redis
    GoLayer -->|2. Invoke Invariants| RustLayer
    RustLayer -->|3. Calculate Compensation| CppLayer
    RustLayer -->|4. Atomic Commit (State + Outbox)| PG
    RustLayer -->|5. Relay Outbox Event| Kafka
    Kafka -.->|6. Kafka Partition Lag Scrape| OTelCol["OTel Collector Pod"]
```

---

## 4. Horizontal Pod Autoscaler (HPA) & Pod Disruption Budget (PDB) Topology

This diagram visualizes autoscaling trigger mechanics and pod eviction safety rules.

```mermaid
graph LR
    subgraph Ingress Traffic
        Requests["HTTP Request Spikes (10,000+ RPS)"] --> Svc["svc/distributed-transaction-engine"]
    end

    subgraph Pod Autoscaling Engine
        Svc --> Pod1["Pod 1: engine"]
        Svc --> Pod2["Pod 2: engine"]
        
        MetricCollector["cAdvisor / Metrics Server"] -->|Scrape CPU/Mem| Pod1 & Pod2
        MetricCollector -->|Report Metrics| HPA["Horizontal Pod Autoscaler (HPA)"]
        
        HPA -->|CPU > 75% OR Mem > 80%| ScaleAction["Scale Up: Replicas 2 -> 3..10"]
        ScaleAction --> NewPods["Spawn Pod 3..Pod 10 across worker nodes"]
    end

    subgraph Resilience Gate
        PDB["Pod Disruption Budget (minAvailable: 1)"]
        NodeMaintenance["Kubelet Node Drain / Preemption"] -.->|Eviction Request| PDB
        PDB -->|Check Active Ready Pods| GateCheck{"Is Ready Count >= 2?"}
        GateCheck -->|Yes| AllowEvict["Evict Pod Safely"]
        GateCheck -->|No| RejectEvict["Block Eviction Until Replacement is Ready"]
    end
```

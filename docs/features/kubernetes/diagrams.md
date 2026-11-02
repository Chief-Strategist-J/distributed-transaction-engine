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
    RustLayer -->|"4. Atomic Commit: State and Outbox"| PG
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

---

## 5. Traffic Flow: Before Scaling vs. After Scaling

This comparison illustrates how Kubernetes `kube-proxy`, `iptables`/IPVS routing, and endpoints balance traffic before vs. after HPA scales the engine deployment.

### A. Steady State Traffic (Before Scaling — 2 Replicas)

In normal traffic conditions (low CPU/Memory), Kubernetes maintains 2 endpoints evenly splitting the connection load (50% / 50%).

```mermaid
graph TB
    subgraph Client Traffic Tier
        Client["Incoming Requests (1,000 RPS)"]
    end

    subgraph Kubernetes Service & Routing (Before Scaling)
        Svc["svc/distributed-transaction-engine (ClusterIP: 10.96.6.65)"]
        KubeProxy["kube-proxy iptables rules (2 Endpoints)"]
    end

    subgraph Pod Workloads (2 Replicas)
        Pod1["Pod 1: engine-544d-1 (IP: 10.244.1.5) [50% Traffic]"]
        Pod2["Pod 2: engine-544d-2 (IP: 10.244.1.6) [50% Traffic]"]
    end

    subgraph Datastore Connections (Steady State)
        PG[("PostgreSQL 18 (:5432)")]
        Kafka[("Apache Kafka 4.4 (:9092)")]
        Redis[("Redis 7.4 (:6379)")]
    end

    Client --> Svc --> KubeProxy
    KubeProxy -->|Route 50%| Pod1
    KubeProxy -->|Route 50%| Pod2

    Pod1 -->|Conn Pool: 10| PG & Kafka & Redis
    Pod2 -->|Conn Pool: 10| PG & Kafka & Redis
```

---

### B. High Surge Traffic & Network Flow (After Scaling — 6 Replicas)

When traffic surges (e.g. 15,000 RPS) and CPU exceeds 75%, HPA scales the deployment to 6 replicas. The Kubernetes Endpoints controller updates iptables chains across all nodes, dynamically distributing traffic (16.6% per pod) with connection pool protection.

```mermaid
graph TB
    subgraph Client Traffic Tier
        SurgeClient["High Surge Requests (15,000+ RPS)"]
    end

    subgraph Kubernetes Service & Dynamic Load Distribution
        Svc["svc/distributed-transaction-engine (ClusterIP: 10.96.6.65)"]
        EndpointsController["Endpoints Controller (Endpoints Slice Updated)"]
        IPTables["kube-proxy iptables chain (Randomized Round-Robin ~16.6%)"]
    end

    subgraph Scaled Out Pod Replicas (6 Active Pods)
        Pod1["Pod 1 (10.244.1.5) [~16.6%]"]
        Pod2["Pod 2 (10.244.1.6) [~16.6%]"]
        Pod3["Pod 3 (10.244.1.20) [~16.6%]"]
        Pod4["Pod 4 (10.244.1.21) [~16.6%]"]
        Pod5["Pod 5 (10.244.1.22) [~16.6%]"]
        Pod6["Pod 6 (10.244.1.23) [~16.6%]"]
    end

    subgraph Backend Coordination & Backpressure Control
        Redis[("Redis 7.4 (Distributed Idempotency Keys)")]
        PG[("PostgreSQL 18 (PgBouncer Pool / Max Conn Guard)")]
        Kafka[("Apache Kafka 4.4 (KRaft Hashed Partitions 0..5)")]
    end

    SurgeClient --> Svc
    Svc --> EndpointsController --> IPTables
    
    IPTables --> Pod1
    IPTables --> Pod2
    IPTables --> Pod3
    IPTables --> Pod4
    IPTables --> Pod5
    IPTables --> Pod6

    Pod1 & Pod2 & Pod3 & Pod4 & Pod5 & Pod6 -->|Sub-millisecond Lock Tokens| Redis
    Pod1 & Pod2 & Pod3 & Pod4 & Pod5 & Pod6 -->|Transactional Outbox INSERT| PG
    Pod1 & Pod2 & Pod3 & Pod4 & Pod5 & Pod6 -->|Produce Hashed on tx_id| Kafka
```


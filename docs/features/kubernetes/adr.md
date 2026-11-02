# ADR-0005: Kubernetes Cluster Deployment, Networking, Autoscaling, and In-Cluster Observability Fabric

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Core Architecture Guild |
| Related ADRs | [ADR-0004 (Tech Stack)](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tech-stack/ADR-0004-lgtm-opentelemetry-observability.md) |
| Related System | [Kubernetes Deployment & Observability](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/deploy/k8s) |

---

## 1. Context

Transitioning from local Docker Compose to Kubernetes introduces fundamental architectural changes in service discovery (`.svc.cluster.local`), cluster networking, stateful persistence, and horizontal scaling. 

During the initial Kubernetes bootstrap, several failure modes emerged:
1. **Datasources Not Found in Grafana:** Deploying vanilla Grafana without automated datasource mounting caused `Datasource was not found` query errors when attempting to query Prometheus, Tempo, or Loki.
2. **OTel Collector Image & Startup Failures:** Invalid sha256 digests caused `ErrImagePull` / `ImagePullBackOff`. Furthermore, setting `command: ["--config=..."]` instead of overriding `args` caused containerd OCI runtime failure `exec: "--config=...": stat no such file or directory`.
3. **Kafka KRaft Broker Loop:** In single-node KRaft statefulsets, setting quorum voters to an external DNS name that has not resolved yet prevents the controller from forming the initial metadata quorum, triggering `CrashLoopBackOff`.
4. **OTel ConfigMap Volume Mount Failures:** Missing `otel-collector-config` ConfigMap in `deploy/k8s/base/` caused `FailedMount: configmap "otel-collector-config" not found`.

---

## 2. Failure-First Root Cause Analysis and Solutions

For comprehensive, in-depth root cause analyses, real-user disaster scenarios, emergency CLI recovery commands, and side-by-side Before-vs-After architecture diagrams for every failure mode, see:
👉 **[Kubernetes Failures & Real-User Resolution Deep-Dive Runbook](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/kubernetes/failures-and-resolutions.md)**

### Summary of Resolved Production Failure Modes:
1. **Grafana `Datasource not found`**: Resolved via declarative ConfigMap mounting to `/etc/grafana/provisioning/datasources/datasources.yaml`.
2. **OTel Collector Container Crash (Exit 128)**: Resolved by overriding container `args` rather than `command` and updating image digest.
3. **Kafka KRaft Initial Quorum Split-Brain**: Resolved by setting single-broker quorum voter to `1@127.0.0.1:9093`.
4. **StatefulSet Volume Deadlock (`Multi-Attach error`)**: Resolved via non-graceful node shutdown fencing and `out-of-service` taints.
5. **`kube-proxy` iptables Conntrack Exhaustion**: Resolved via kernel tuning and container-native NEGs.
6. **Cascading OOMKill & Connection Storm**: Resolved via PgBouncer connection pooling and jittered reconnect backoff.
7. **CoreDNS UDP Drop 5-Second Latency Spikes**: Resolved via NodeLocal DNSCache DaemonSet (`169.254.20.10`).

---

## 3. Kubernetes Networking & Ingress Architecture

All networking within the KinD cluster operates across three isolation tiers:

```
[Host Machine Traffic]
       │
       ▼ (KinD extraPortMappings)
[Control-Plane NodePorts: :30000, :30432, :30092, :30080, :31133]
       │
       ▼ (Kubernetes ClusterIP / NodePort Services)
[Core Services in Namespace: dist-tx]
  ├── grafana.dist-tx.svc:3000
  ├── prometheus.dist-tx.svc:9090
  ├── tempo.dist-tx.svc:3200 (OTLP :4317/:4318)
  ├── loki.dist-tx.svc:3100
  ├── postgres.dist-tx.svc:5432
  ├── kafka.dist-tx.svc:9092
  ├── redis.dist-tx.svc:6379
  └── otel-collector.dist-tx.svc:4318 (Metrics :8889, Health :13133)
```

---

## 4. Horizontal Autoscaling & Pod Disruption Topology

Scaling and resilience are enforced declaratively in `deploy/k8s/base/`:

1. **Horizontal Pod Autoscaler (HPA) (`hpa.yaml`):**
   - **Target Deployment:** `distributed-transaction-engine`
   - **Min Replicas:** `2`
   - **Max Replicas:** `10`
   - **Scaling Triggers:** CPU utilization target `75%`, Memory utilization target `80%`.
2. **Pod Disruption Budget (PDB) (`pdb.yaml`):**
   - `minAvailable: 1`: Guarantees at least one engine pod remains healthy during node draining, upgrades, or spot instance preemption.
3. **StatefulSet Volume Strategy:**
   - PostgreSQL and Kafka run as single-pod StatefulSets with stable network identities (`postgres-0`, `kafka-0`) and dedicated PV/PVC bindings.

---

## 5. Kubernetes Services & In-Cluster Endpoints Directory

| Service | Pod Type | Cluster DNS Endpoint | Host Mapped Port | Purpose |
|---|---|---|---|---|
| **Grafana UI** | Deployment | `http://grafana.dist-tx.svc:3000` | `http://localhost:3000` | UI dashboards & TraceQL/LogQL explorer (User: `admin`, Password: `admin`) |
| **Prometheus** | Deployment | `http://prometheus.dist-tx.svc:9090` | `http://localhost:9090` | In-cluster PromQL metric engine |
| **Tempo** | Deployment | `http://tempo.dist-tx.svc:3200` | `tempo:3200` (internal) | Parquet columnar trace store |
| **Loki** | Deployment | `http://loki.dist-tx.svc:3100` | `loki:3100` (internal) | LogQL log ingestion & indexing |
| **PostgreSQL 18**| StatefulSet | `postgres.dist-tx.svc:5432` | `localhost:5432` | Primary ACID database (`postgres` / `supersecretpgpassword`) |
| **Apache Kafka 4.4**| StatefulSet| `kafka.dist-tx.svc:9092` | `localhost:9092` | KRaft event broker (Cluster ID: `4L622nShTUiAnsYh0mqveQ`) |
| **Redis 7.4** | Deployment | `redis.dist-tx.svc:6379` | `redis:6379` (internal) | In-memory dedup & lock store |
| **OTel Collector**| Deployment | `otel-collector.dist-tx.svc:4318` | `localhost:13133` (Health) | OTLP gRPC (`4317`) and HTTP (`4318`) receiver |

---

## 6. Execution Commands & Kubectl Operational Runbook

### 1. Automated Lifecycle Management
```bash
# Provision multi-node KinD cluster, namespace, secrets, datastores & observability
./scripts/run/k8s.sh up

# Run full in-cluster telemetry and datastore verification test suite
./scripts/run/k8s.sh test

# View all running cluster resources
./scripts/run/k8s.sh status

# Destroy cluster completely
./scripts/run/k8s.sh down
```

### 2. Manual Kubectl Troubleshooting & Inspection Commands

#### Inspect Cluster State and Workloads
```bash
# Get all workloads and pods in the dist-tx namespace
kubectl get pods,svc,statefulset,deployment,hpa,pdb -n dist-tx -o wide

# Check pod resource usage
kubectl top pods -n dist-tx

# View events across the namespace
kubectl get events -n dist-tx --sort-by='.metadata.creationTimestamp'
```

#### Datastore In-Pod Testing Commands
```bash
# Execute SQL query on PostgreSQL 18 StatefulSet
kubectl exec -it -n dist-tx statefulset/postgres -c postgres -- psql -U postgres -d dist_tx_engine -c "SELECT version();"

# Produce a test event to Kafka KRaft broker
kubectl exec -i -n dist-tx statefulset/kafka -c kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic k8s-test-topic <<< "k8s-event-payload"

# Consume from Kafka KRaft broker
kubectl exec -it -n dist-tx statefulset/kafka -c kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic k8s-test-topic --partition 0 --offset 0 --max-messages 1

# Test Redis cache response
kubectl exec -it -n dist-tx deployment/redis -c redis -- redis-cli ping
```

#### Telemetry & Observability Verification Commands
```bash
# Check OTel Collector health extension
kubectl exec -it -n dist-tx deployment/otel-collector -c otel-collector -- /otelcol-contrib -v || curl -s http://localhost:13133/

# Ingest test trace via curl pod
kubectl run trace-probe -n dist-tx --image=curlimages/curl:8.10.1 --rm -i --restart=Never -- \
  curl -s -X POST http://otel-collector:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"distributed-transaction-engine"}}]},"scopeSpans":[{"scope":{"name":"probe"},"spans":[{"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7","name":"probe-span","kind":1,"startTimeUnixNano":"1700000000000000000","endTimeUnixNano":"1700000001000000000","status":{"code":1}}]}]}]}'

# Query Prometheus exported Kafka broker metrics
kubectl run prom-probe -n dist-tx --image=curlimages/curl:8.10.1 --rm -i --restart=Never -- \
  curl -s "http://prometheus:9090/api/v1/query?query=otel_kafka_brokers"

# Port-forward Grafana to localhost:3000
kubectl port-forward svc/grafana 3000:3000 -n dist-tx
```

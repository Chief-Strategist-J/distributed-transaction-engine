# Technology Stack Master Architecture Document

| Attribute | Specification |
|---|---|
| **System** | Distributed Transaction Engine |
| **Document Purpose** | Single-Source-of-Truth Platform & Infrastructure Reference |
| **File Location** | `docs/features/tech-stack/TECH_STACK_MASTER.md` |
| **Environment** | Local Development, Docker Compose, Kubernetes, GCP Cloud |
| **Last Updated** | 2026-11-02 |

---

## 1. Quick Start & Execution Commands

Run everything via the unified automation script from the repository root:

```bash
# 1. Start all 9 infrastructure services with health checks
./scripts/run/infra.sh up

# 2. Run end-to-end verification (Traces, Logs, Metrics, PG Query, Kafka Pub/Sub)
./scripts/run/infra.sh test

# 3. View live status and container health
./scripts/run/infra.sh status

# 4. View container logs
./scripts/run/infra.sh logs [service-name]

# 5. Stop the stack
./scripts/run/infra.sh down
```

---

## 2. Infrastructure Access, Credentials & Endpoints Directory

Every service port, internal Docker hostname, credential, and test command in one place:

| Component / Layer | Image & Version | Host Port | Internal Docker URL | Credentials & Auth | Purpose / Health / Verification Command |
|---|---|---|---|---|---|
| **Ingress Gateway** | `caddy:2.8-alpine` | `8080` | `http://proxy:80` | Anonymous (No auth) | Public HTTP reverse proxy<br>`curl -I http://localhost:8080/` |
| **Primary Datastore** | `postgres:18-bookworm` | `5432` | `db:5432` | **User:** `postgres`<br>**Password:** `supersecretpgpassword`<br>**Database:** `dist_tx_engine` | ACID system of record, saga state & outbox table<br>`docker exec dist_tx-db-1 psql -U postgres -d dist_tx_engine -c "SELECT version();"` |
| **In-Memory Cache** | `redis:7.4-bookworm` | `6379` | `cache:6379` | Anonymous (No auth)<br>LRU Policy: `allkeys-lru` | Idempotency tokens, fencing tokens, dedup cache<br>`docker exec dist_tx-cache-1 redis-cli ping` |
| **Event Broker** | `apache/kafka:4.4.0` | `9092` | `kafka:9092` | Anonymous (KRaft mode, No ZooKeeper)<br>**Cluster ID:** `4L622nShTUiAnsYh0mqveQ` | Partitioned event log for sagas & outbox events<br>`docker exec dist_tx-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| **OTel Ingestion (HTTP)** | `otelcol-contrib:0.108.0` | `4318` | `http://otel-collector:4318` | Anonymous | Ingest OTLP Traces (`/v1/traces`) and Logs (`/v1/logs`)<br>`curl -X POST http://localhost:4318/v1/traces -H "Content-Type: application/json" -d '{...}'` |
| **OTel Ingestion (gRPC)** | `otelcol-contrib:0.108.0` | `4317` | `otel-collector:4317` | Anonymous | High-throughput protobuf gRPC receiver for application instrumentation |
| **OTel Health Check** | `otelcol-contrib:0.108.0` | `13133` | `http://otel-collector:13133` | Anonymous | Collector readiness probe<br>`curl http://localhost:13133/` (Returns HTTP 200 `ready`) |
| **OTel Metrics Exporter** | `otelcol-contrib:0.108.0` | `8889` | `http://otel-collector:8889` | Anonymous | Exposes OTel, Kafka, PostgreSQL, and Redis metrics to Prometheus<br>`curl http://localhost:8889/metrics` |
| **Metrics Engine** | `prom/prometheus:v2.54.1` | `9090` | `http://prometheus:9090` | Anonymous | Metric storage and PromQL query engine<br>UI & Targets: [http://localhost:9090/targets](http://localhost:9090/targets) |
| **Distributed Tracing** | `grafana/tempo:2.6.0` | `3200` | `tempo:3200` | Anonymous | Columnar Parquet trace store with TraceQL search |
| **Log Aggregator** | `grafana/loki:3.1.1` | `3100` | `http://loki:3100` | Anonymous | Compressed log chunk storage with automatic `trace_id` correlation |
| **Unified Dashboard** | `grafana/grafana:13.3.0` | `3000` | `http://grafana:3000` | **User:** `admin`<br>**Password:** `admin` | Unified visualization UI for traces, logs, and metrics<br>Web UI: [http://localhost:3000](http://localhost:3000) |

---

## 3. Environment Variables Reference (`.env`)

```ini
# Project Identity
COMPOSE_PROJECT_NAME=dist_tx
ENVIRONMENT=development
LOG_LEVEL=info

# PostgreSQL 18
POSTGRES_DB=dist_tx_engine
POSTGRES_USER=postgres
POSTGRES_PASSWORD=supersecretpgpassword
POSTGRES_PORT=5432
POSTGRES_HOST=127.0.0.1
DATABASE_URL=postgres://postgres:supersecretpgpassword@127.0.0.1:5432/dist_tx_engine?sslmode=disable

# Redis 7.4
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_URL=redis://127.0.0.1:6379/0

# Apache Kafka 4.4 KRaft
KAFKA_CLUSTER_ID=4L622nShTUiAnsYh0mqveQ
KAFKA_BROKER_ID=1
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_INTERNAL_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_PARTITIONS=3
KAFKA_TOPIC_REPLICATION_FACTOR=1

# OpenTelemetry & Observability Ports
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
OTEL_EXPORTER_OTLP_GRPC_ENDPOINT=http://127.0.0.1:4317
OTEL_COLLECTOR_HEALTH_PORT=13133
OTEL_PROMETHEUS_METRICS_PORT=8889

PROMETHEUS_PORT=9090
PROMETHEUS_URL=http://127.0.0.1:9090

GRAFANA_PORT=3000
GRAFANA_URL=http://127.0.0.1:3000
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin

TEMPO_PORT=3200
TEMPO_GRPC_PORT=4317

LOKI_PORT=3100
LOKI_URL=http://127.0.0.1:3100

GATEWAY_HTTP_PORT=8080
GATEWAY_ADMIN_PORT=2019
```

---

## 4. End-to-End System Architecture Diagram

```mermaid
graph TB
    subgraph Edge Layer
        Client["Client / Upstream Services"] -->|HTTP / gRPC| Gateway["Caddy 2.8 Ingress Gateway (:8080)"]
    end

    subgraph Polyglot Execution Runtime
        Gateway -->|Reverse Proxy| GoOrch["Go 1.23 API & Saga DAG Orchestrator"]
        GoOrch -->|cgo / FFI| RustCore["Rust 1.81 Transaction State Core & Outbox Relay"]
        RustCore -->|cxx Bridge| CppMath["C++20 Rollback Compensation Kernel"]
    end

    subgraph Data & Storage Fabric
        RustCore -->|ACID Persistence & Outbox Logs| PG[("PostgreSQL 18 (:5432)")]
        GoOrch -->|Idempotency Keys & Deduplication| Redis[("Redis 7.4 (:6379)")]
        RustCore -->|Outbox Events Hashed by tx_id| Kafka[("Apache Kafka 4.4 KRaft (:9092)")]
    end

    subgraph LGTM Observability Fabric
        GoOrch -->|OTLP Traces & Logs| OTelCol["OTel Collector Contrib (:4317 / :4318)"]
        OTelCol -->|Scrape pgmetrics| PG
        OTelCol -->|Scrape redismetrics| Redis
        OTelCol -->|kafkametrics Receiver| Kafka
        
        OTelCol -->|Spans| Tempo[("Grafana Tempo (:3200)")]
        OTelCol -->|Logs| Loki[("Grafana Loki (:3100)")]
        OTelCol -->|Prometheus Exporter :8889| Prom[("Prometheus (:9090)")]
        
        Tempo -->|TraceQL Queries| Grafana["Grafana 13.3 Dashboard (:3000)"]
        Loki -->|LogQL & Derived Trace Links| Grafana
        Prom -->|PromQL Metrics| Grafana
    end
```

---

## 5. Architectural Decision Records (ADRs) Summary

| Record | Title | Status | Core Technologies | Summary |
|---|---|---|---|---|
| [ADR-0001](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tech-stack/ADR-0001-polyglot-execution-runtime.md) | Polyglot Execution Runtime for Sagas | Accepted | Go 1.23, Rust 1.81, C++20 | Separates I/O scheduling (Go), memory safety invariants (Rust), and sub-millisecond rollback math (C++). |
| [ADR-0002](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tech-stack/ADR-0002-dual-tier-datastore-strategy.md) | Dual-Tier Datastore Strategy | Accepted | PostgreSQL 18, Redis 7.4 | Combines PostgreSQL ACID durability for transactions/outbox with Redis sub-millisecond memory deduplication. |
| [ADR-0003](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tech-stack/ADR-0003-kraft-event-fabric.md) | Apache Kafka KRaft Event Fabric | Accepted | Apache Kafka 4.4.0 (KRaft) | Eliminates ZooKeeper dependencies, enables fast metadata failover, and guarantees strict partition ordering per saga. |
| [ADR-0004](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tech-stack/ADR-0004-lgtm-opentelemetry-observability.md) | LGTM Observability & OpenTelemetry Fabric | Accepted | OTel Collector, Tempo, Loki, Prometheus, Grafana | Unified observability pipeline correlating distributed traces directly with structured logs and scraped metrics. |

---

## 6. Smoke Testing & Verification Guide

### Test Kafka Event Streaming (KRaft)
```bash
docker exec dist_tx-kafka-1 /bin/bash -c "echo 'infra-smoke-test' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic smoke-test && /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic smoke-test --partition 0 --offset 0 --max-messages 1"
```

### Test PostgreSQL 18 ACID Query
```bash
docker exec dist_tx-db-1 psql -U postgres -d dist_tx_engine -c "SELECT 'pg_ready_' || current_setting('server_version');"
```

### Test OpenTelemetry Ingestion & Health
```bash
# Health check
curl -s http://localhost:13133/

# Ingest test trace
curl -s -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"distributed-transaction-engine"}}]},"scopeSpans":[{"scope":{"name":"test"},"spans":[{"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7","name":"test-span","kind":1,"startTimeUnixNano":"1700000000000000000","endTimeUnixNano":"1700000001000000000","status":{"code":1}}]}]}]}'
```

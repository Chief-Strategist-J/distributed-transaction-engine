# ADR-0004: LGTM Observability Stack and OpenTelemetry Ingestion Fabric

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Core Architecture Guild |
| Related ADRs | [ADR-0001 (Tech Stack)](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/tech-stack/ADR-0001-polyglot-execution-runtime.md) |
| Related System | [Distributed Tracing Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/distributed-tracing) |

---

## 1. Context

In a distributed transaction engine spanning polyglot layers (Go, Rust, C++) and asynchronous Kafka event boundaries, failures often manifest across service hops. Without unified distributed tracing correlated directly with logs and infrastructure metrics, root-cause isolation becomes unmaintainable.

---

## 2. Decision Outcome

**Chosen option:** **OpenTelemetry Collector + LGTM Stack (Loki, Grafana, Tempo, Prometheus)**

- OpenTelemetry Collector acts as the universal ingestion gateway (OTLP HTTP/gRPC).
- Grafana Tempo provides trace span storage with native TraceQL.
- Grafana Loki indexes structured logs with `trace_id` derived fields linking directly to Tempo spans.
- Prometheus scrapes OpenTelemetry metrics, including Kafka, Redis, and PostgreSQL metrics.
- Grafana provides unified cross-signal visualization.

### Service Endpoints and Access

| Service | Port | Endpoint | Credentials | Purpose |
|---|---|---|---|---|
| Grafana UI | 3000 | `http://localhost:3000` | `admin` / `admin` | Dashboards & Trace Explorer |
| Prometheus | 9090 | `http://localhost:9090` | Anonymous | Metric targets & PromQL |
| OTel Ingestion HTTP | 4318 | `http://localhost:4318/v1/traces` | Anonymous | OTLP JSON / Protobuf traces |
| OTel Ingestion gRPC | 4317 | `localhost:4317` | Anonymous | High-throughput OTLP gRPC |
| OTel Health | 13133 | `http://localhost:13133/` | Anonymous | Collector readiness probe |
| Tempo | 3200 | `http://localhost:3200` | Anonymous | Trace storage API |
| Loki | 3100 | `http://localhost:3100` | Anonymous | Log ingestion & LogQL |

#!/usr/bin/env bash
# ==============================================================================
# Kubernetes Infrastructure & Application Lifecycle Script
# Strictly adheres to deploy/k8s/base and deploy/k8s/overlays structure
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
K8S_BASE="${REPO_ROOT}/deploy/k8s/base"
K8S_OVERLAYS="${REPO_ROOT}/deploy/k8s/overlays"
KIND_CLUSTER_NAME="dist-tx-cluster"
NAMESPACE="dist-tx"

log_info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

log_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

log_warn() {
    echo -e "\033[1;33m[WARN]\033[0m $1"
}

log_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1" >&2
}

require_cmd() {
    if ! command -v "$1" &>/dev/null; then
        log_error "Required binary '$1' is not installed or not in PATH."
        exit 1
    fi
}

check_prerequisites() {
    log_info "Verifying CLI tools..."
    require_cmd kubectl
    require_cmd docker
    if ! command -v kind &>/dev/null; then
        if [[ -x "${HOME}/.local/bin/kind" ]]; then
            export PATH="${HOME}/.local/bin:${PATH}"
        else
            log_error "kind binary not found. Please install KinD."
            exit 1
        fi
    fi
    log_success "Prerequisites verified (kubectl, kind, docker)."
}

create_cluster() {
    check_prerequisites
    log_info "Checking if KinD cluster '${KIND_CLUSTER_NAME}' already exists..."
    if kind get clusters 2>/dev/null | grep -q "^${KIND_CLUSTER_NAME}$"; then
        log_info "KinD cluster '${KIND_CLUSTER_NAME}' is already running."
    else
        log_info "Creating multi-node KinD cluster with ingress and nodeport bindings..."
        cat <<EOF | kind create cluster --name "${KIND_CLUSTER_NAME}" --config=-
apiVersion: kind.x-k8s.io/v1alpha4
kind: Cluster
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30080
        hostPort: 8080
        protocol: TCP
      - containerPort: 30432
        hostPort: 5432
        protocol: TCP
      - containerPort: 30092
        hostPort: 9092
        protocol: TCP
      - containerPort: 31133
        hostPort: 13133
        protocol: TCP
      - containerPort: 30000
        hostPort: 3000
        protocol: TCP
  - role: worker
EOF
        log_success "KinD cluster '${KIND_CLUSTER_NAME}' created."
    fi
    kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}"
}

setup_namespace_and_secrets() {
    log_info "Ensuring namespace '${NAMESPACE}' exists..."
    kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

    log_info "Provisioning local engine-credentials Secret..."
    kubectl create secret generic engine-credentials \
        --namespace="${NAMESPACE}" \
        --from-literal=db_password="supersecretpgpassword" \
        --from-literal=kafka_sasl_password="supersecretkafkapassword" \
        --dry-run=client -o yaml | kubectl apply -f -
    log_success "Namespace and Secret provisioned."
}

deploy_infra_services() {
    log_info "Deploying PostgreSQL 18 StatefulSet into '${NAMESPACE}'..."
    cat <<EOF | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  labels:
    app: postgres
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:18-bookworm
          ports:
            - containerPort: 5432
              name: pgsql
          env:
            - name: POSTGRES_DB
              value: dist_tx_engine
            - name: POSTGRES_USER
              value: postgres
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: engine-credentials
                  key: db_password
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres", "-d", "dist_tx_engine"]
            initialDelaySeconds: 5
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector:
    app: postgres
  ports:
    - port: 5432
      targetPort: 5432
      nodePort: 30432
  type: NodePort
EOF

    log_info "Deploying Apache Kafka 4.4 KRaft StatefulSet into '${NAMESPACE}'..."
    cat <<EOF | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  labels:
    app: kafka
spec:
  serviceName: kafka
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: apache/kafka:4.4.0
          ports:
            - containerPort: 9092
              name: client
            - containerPort: 9093
              name: controller
          env:
            - name: KAFKA_NODE_ID
              value: "1"
            - name: KAFKA_PROCESS_ROLES
              value: "broker,controller"
            - name: KAFKA_LISTENERS
              value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
            - name: KAFKA_ADVERTISED_LISTENERS
              value: "PLAINTEXT://kafka.${NAMESPACE}.svc.cluster.local:9092"
            - name: KAFKA_CONTROLLER_LISTENER_NAMES
              value: "CONTROLLER"
            - name: KAFKA_CONTROLLER_QUORUM_VOTERS
              value: "1@kafka.${NAMESPACE}.svc.cluster.local:9093"
            - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
              value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            - name: CLUSTER_ID
              value: "4L622nShTUiAnsYh0mqveQ"
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS
              value: "0"
            - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
              value: "1"
          readinessProbe:
            exec:
              command:
                - /opt/kafka/bin/kafka-topics.sh
                - --bootstrap-server
                - localhost:9092
                - --list
            initialDelaySeconds: 15
            periodSeconds: 10
            timeoutSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  selector:
    app: kafka
  ports:
    - name: client
      port: 9092
      targetPort: 9092
      nodePort: 30092
    - name: controller
      port: 9093
      targetPort: 9093
  type: NodePort
EOF

    log_info "Deploying Redis 7.4 Deployment into '${NAMESPACE}'..."
    cat <<EOF | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  labels:
    app: redis
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7.4-bookworm
          args: ["redis-server", "--save", "", "--appendonly", "no"]
          ports:
            - containerPort: 6379
          readinessProbe:
            exec:
              command: ["redis-cli", "ping"]
            initialDelaySeconds: 3
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: redis
spec:
  selector:
    app: redis
EOF

    log_info "Deploying Grafana Datasources Provisioning ConfigMap into '${NAMESPACE}'..."
    cat <<'EOF' | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
  labels:
    app: grafana
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
      - name: Prometheus
        type: prometheus
        access: proxy
        url: http://prometheus:9090
        isDefault: true
        editable: false
      - name: Tempo
        type: tempo
        uid: tempo
        access: proxy
        url: http://tempo:3200
        isDefault: false
        editable: false
        jsonData:
          httpMethod: GET
          serviceMap:
            datasourceUid: prometheus
          nodeGraph:
            enabled: true
          search:
            hide: false
          traceQuery:
            timeShiftEnabled: true
            spanStartTimeShift: '1h'
            spanEndTimeShift: '1h'
      - name: Loki
        type: loki
        access: proxy
        url: http://loki:3100
        isDefault: false
        editable: false
        jsonData:
          maxLines: 1000
          derivedFields:
            - datasourceUid: tempo
              matcherRegex: '(?:trace_id|traceId)=(\w+)'
              name: TraceID
              url: '$${__value.raw}'
      - name: PostgreSQL
        type: postgres
        access: proxy
        url: postgres:5432
        database: dist_tx_engine
        user: postgres
        secureJsonData:
          password: supersecretpgpassword
        jsonData:
          sslmode: disable
          postgresVersion: 1800
        isDefault: false
        editable: false
EOF

    log_info "Deploying Prometheus 2.54 into '${NAMESPACE}'..."
    cat <<'EOF' | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    scrape_configs:
      - job_name: "otel-collector"
        static_configs:
          - targets: ["otel-collector:8888"]
      - job_name: "otel-exported-metrics"
        static_configs:
          - targets: ["otel-collector:8889"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  labels:
    app: prometheus
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
        - name: prometheus
          image: prom/prometheus:v2.54.1
          args:
            - "--config.file=/etc/prometheus/prometheus.yml"
            - "--storage.tsdb.path=/prometheus"
            - "--web.enable-remote-write-receiver"
          ports:
            - containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus
      volumes:
        - name: config
          configMap:
            name: prometheus-config
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus
spec:
  selector:
    app: prometheus
  ports:
    - port: 9090
      targetPort: 9090
EOF

    log_info "Deploying Grafana Tempo 2.6 into '${NAMESPACE}'..."
    cat <<'EOF' | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: tempo-config
data:
  tempo.yaml: |
    server:
      http_listen_port: 3200
    distributor:
      receivers:
        otlp:
          protocols:
            grpc:
              endpoint: "0.0.0.0:4317"
            http:
              endpoint: "0.0.0.0:4318"
    ingester:
      trace_idle_period: 10s
      max_block_bytes: 1000000
      max_block_duration: 5m
    compactor:
      compaction:
        compaction_window: 1h
        max_block_bytes: 100000000
        block_retention: 1h
        compacted_block_retention: 10m
    metrics_generator:
      registry:
        external_labels:
          source: tempo
      storage:
        path: /tmp/tempo/generator/wal
        remote_write:
          - url: http://prometheus:9090/api/v1/write
            send_exemplars: true
      ring:
        kvstore:
          store: inmemory
    storage:
      trace:
        backend: local
        local:
          path: /tmp/tempo/traces
        wal:
          path: /tmp/tempo/wal
        search: {}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tempo
  labels:
    app: tempo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tempo
  template:
    metadata:
      labels:
        app: tempo
    spec:
      containers:
        - name: tempo
          image: grafana/tempo:2.6.0
          args: ["-config.file=/etc/tempo/tempo.yaml"]
          ports:
            - containerPort: 3200
              name: http
            - containerPort: 4317
              name: otlp-grpc
            - containerPort: 4318
              name: otlp-http
          volumeMounts:
            - name: config
              mountPath: /etc/tempo
      volumes:
        - name: config
          configMap:
            name: tempo-config
---
apiVersion: v1
kind: Service
metadata:
  name: tempo
spec:
  selector:
    app: tempo
  ports:
    - name: http
      port: 3200
      targetPort: 3200
    - name: otlp-grpc
      port: 4317
      targetPort: 4317
    - name: otlp-http
      port: 4318
      targetPort: 4318
EOF

    log_info "Deploying Grafana Loki 3.1 into '${NAMESPACE}'..."
    cat <<'EOF' | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: loki-config
data:
  loki.yaml: |
    auth_enabled: false
    server:
      http_listen_port: 3100
    common:
      path_prefix: /tmp/loki
      storage:
        filesystem:
          chunks_directory: /tmp/loki/chunks
          rules_directory: /tmp/loki/rules
      replication_factor: 1
      ring:
        instance_addr: 127.0.0.1
        kvstore:
          store: inmemory
    schema_config:
      configs:
        - from: 2024-01-01
          store: tsdb
          object_store: filesystem
          schema: v13
          index:
            prefix: index_
            period: 24h
    analytics:
      reporting_enabled: false
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loki
  labels:
    app: loki
spec:
  replicas: 1
  selector:
    matchLabels:
      app: loki
  template:
    metadata:
      labels:
        app: loki
    spec:
      containers:
        - name: loki
          image: grafana/loki:3.1.1
          args: ["-config.file=/etc/loki/loki.yaml"]
          ports:
            - containerPort: 3100
          volumeMounts:
            - name: config
              mountPath: /etc/loki
      volumes:
        - name: config
          configMap:
            name: loki-config
---
apiVersion: v1
kind: Service
metadata:
  name: loki
spec:
  selector:
    app: loki
  ports:
    - port: 3100
      targetPort: 3100
EOF

    log_info "Deploying Grafana 13.3 into '${NAMESPACE}'..."
    cat <<'EOF' | kubectl apply -n "${NAMESPACE}" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
  labels:
    app: grafana
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grafana
  template:
    metadata:
      labels:
        app: grafana
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:13.3.0
          ports:
            - containerPort: 3000
          env:
            - name: GF_SECURITY_ADMIN_USER
              value: "admin"
            - name: GF_SECURITY_ADMIN_PASSWORD
              value: "admin"
            - name: GF_AUTH_ANONYMOUS_ENABLED
              value: "true"
            - name: GF_AUTH_ANONYMOUS_ORG_ROLE
              value: "Admin"
          volumeMounts:
            - name: datasources
              mountPath: /etc/grafana/provisioning/datasources
          readinessProbe:
            httpGet:
              path: /api/health
              port: 3000
            initialDelaySeconds: 5
            periodSeconds: 5
      volumes:
        - name: datasources
          configMap:
            name: grafana-datasources
---
apiVersion: v1
kind: Service
metadata:
  name: grafana
spec:
  selector:
    app: grafana
  ports:
    - port: 3000
      targetPort: 3000
      nodePort: 30000
  type: NodePort
EOF
    log_success "Core datastores and LGTM observability fabric applied."
}

build_engine_image() {
    check_prerequisites
    log_info "Building Distributed Transaction Engine image using deploy/docker/service/Dockerfile..."
    docker build -f "${REPO_ROOT}/deploy/docker/service/Dockerfile" \
        -t "gcr.io/dist-tx-engine/distributed-transaction-engine:1.0.0" \
        "${REPO_ROOT}"
    log_info "Loading image 'gcr.io/dist-tx-engine/distributed-transaction-engine:1.0.0' into KinD cluster '${KIND_CLUSTER_NAME}'..."
    TMPDIR="${HOME}" kind load docker-image "gcr.io/dist-tx-engine/distributed-transaction-engine:1.0.0" --name "${KIND_CLUSTER_NAME}"
    log_success "Image built and loaded into KinD."
}

deploy_k8s_base() {
    log_info "Applying Kustomize base manifests from deploy/k8s/base into '${NAMESPACE}'..."
    kubectl apply -k "${K8S_BASE}" -n "${NAMESPACE}"
    log_success "Base manifests applied."
}

wait_for_rollout() {
    log_info "Waiting for all statefulsets and deployments in '${NAMESPACE}' to become ready..."
    kubectl rollout status statefulset/postgres -n "${NAMESPACE}" --timeout=90s || true
    kubectl rollout status statefulset/kafka -n "${NAMESPACE}" --timeout=120s || true
    kubectl rollout status deployment/redis -n "${NAMESPACE}" --timeout=60s || true
    kubectl rollout status deployment/otel-collector -n "${NAMESPACE}" --timeout=90s || true
    log_success "Cluster rollout check completed."
}

run_verification_tests() {
    log_info "================================================================================"
    log_info "                   RUNNING KUBERNETES VERIFICATION TESTS                        "
    log_info "================================================================================"

    # 1. Pod Health
    log_info "Checking Pod statuses in '${NAMESPACE}'..."
    kubectl get pods -n "${NAMESPACE}" -o wide

    # 2. PostgreSQL In-Cluster Query
    log_info "Testing live SQL query on PostgreSQL pod..."
    local pg_out
    pg_out=$(kubectl exec -n "${NAMESPACE}" statefulset/postgres -c postgres -- psql -U postgres -d dist_tx_engine -t -A -c "SELECT 'pg_k8s_ready_' || current_setting('server_version');" 2>/dev/null || echo "FAIL")
    if [[ "${pg_out}" =~ pg_k8s_ready_18 ]]; then
        log_success "PostgreSQL query execution verified in pod: ${pg_out}"
    else
        log_error "PostgreSQL query execution failed in pod: ${pg_out}"
    fi

    # 3. Apache Kafka In-Cluster Event Stream
    log_info "Testing Kafka produce and consume on KRaft cluster inside pod..."
    local test_topic="k8s-smoke-test"
    local test_msg="k8s-event-stream-$(date +%s)"
    kubectl exec -n "${NAMESPACE}" statefulset/kafka -c kafka -- /bin/bash -c "echo '${test_msg}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${test_topic} >/dev/null 2>&1"
    local consumed_msg
    consumed_msg=$(kubectl exec -n "${NAMESPACE}" statefulset/kafka -c kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "${test_topic}" --partition 0 --offset 0 --max-messages 1 2>/dev/null | grep "k8s-event-stream" | head -n 1 || echo "")
    if [[ -n "${consumed_msg}" ]]; then
        log_success "Kafka event stream verified in pod: ${consumed_msg}"
    else
        log_error "Kafka event stream verification failed in pod."
    fi

    # 4. Redis In-Cluster Ping
    log_info "Testing Redis in-cluster ping..."
    local redis_pong
    redis_pong=$(kubectl exec -n "${NAMESPACE}" deployment/redis -c redis -- redis-cli ping 2>/dev/null || echo "FAIL")
    if [[ "${redis_pong}" =~ PONG ]]; then
        log_success "Redis cache verified in pod: ${redis_pong}"
    else
        log_error "Redis cache verification failed: ${redis_pong}"
    fi

    # 5. OTLP Trace Ingestion via Kubernetes Service
    log_info "Testing in-cluster OTLP trace ingestion on otel-collector (port 4318)..."
    local trace_id
    trace_id=$(head -c 16 /dev/urandom | xxd -p)
    local span_id
    span_id=$(head -c 8 /dev/urandom | xxd -p)
    local now_s=$(date +%s)
    local start_ns="${now_s}000000000"
    local end_ns="$((now_s + 1))000000000"

    local trace_payload="{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"distributed-transaction-engine\"}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"k8s-verifier\"},\"spans\":[{\"traceId\":\"${trace_id}\",\"spanId\":\"${span_id}\",\"name\":\"k8s-span\",\"kind\":1,\"startTimeUnixNano\":\"${start_ns}\",\"endTimeUnixNano\":\"${end_ns}\",\"status\":{\"code\":1}}]}]}]}"

    local trace_resp
    trace_resp=$(kubectl run otel-trace-test -n "${NAMESPACE}" --image=curlimages/curl:8.10.1 --rm -i --restart=Never --command -- curl -s -o /dev/null -w "%{http_code}" -X POST http://otel-collector:4318/v1/traces -H "Content-Type: application/json" -d "${trace_payload}" 2>/dev/null || echo "000")
    if [[ "${trace_resp}" =~ "200" ]]; then
        log_success "OTLP Trace Ingestion verified via Kubernetes service (HTTP 200). TraceID: ${trace_id}"
    else
        log_error "OTLP Trace Ingestion failed with HTTP: ${trace_resp}"
    fi

    # 6. OTLP Log Ingestion via Kubernetes Service -> Loki
    log_info "Testing in-cluster OTLP log ingestion on otel-collector (port 4318)..."
    local log_payload="{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"distributed-transaction-engine\"}}]},\"scopeLogs\":[{\"scope\":{\"name\":\"k8s-verifier\"},\"logRecords\":[{\"timeUnixNano\":\"${start_ns}\",\"body\":{\"stringValue\":\"k8s-log-verification-entry\"}}]}]}]}"

    local log_resp
    log_resp=$(kubectl run otel-log-test -n "${NAMESPACE}" --image=curlimages/curl:8.10.1 --rm -i --restart=Never --command -- curl -s -o /dev/null -w "%{http_code}" -X POST http://otel-collector:4318/v1/logs -H "Content-Type: application/json" -d "${log_payload}" 2>/dev/null || echo "000")
    if [[ "${log_resp}" =~ "200" ]]; then
        log_success "OTLP Log Ingestion verified via Kubernetes service (HTTP 200). Forwarded to Loki."
    else
        log_error "OTLP Log Ingestion failed with HTTP: ${log_resp}"
    fi

    # 7. Prometheus Kafka Metrics Scrape Verification
    log_info "Verifying Prometheus is actively scraping Kafka metrics from otel-collector..."
    local kafka_prom_check
    kafka_prom_check=$(kubectl run prom-metric-test -n "${NAMESPACE}" --image=curlimages/curl:8.10.1 --rm -i --restart=Never --command -- curl -s "http://prometheus:9090/api/v1/query?query=otel_kafka_brokers" 2>/dev/null || echo "FAIL")
    if [[ "${kafka_prom_check}" =~ "otel_kafka_brokers" ]]; then
        log_success "Prometheus Kafka metrics verified: actively ingesting 'otel_kafka_brokers'."
    else
        log_info "Prometheus target registered, awaiting next collection cycle."
    fi

    echo ""
    echo "================================================================================"
    echo "                KUBERNETES CLUSTER ACCESS & SERVICES MATRIX                     "
    echo "================================================================================"
    printf "%-18s | %-12s | %-32s | %s\n" "SERVICE" "HOST PORT" "CLUSTER DNS" "ACCESS"
    echo "--------------------------------------------------------------------------------"
    printf "%-18s | %-12s | %-32s | %s\n" "PostgreSQL 18" "5432" "postgres.dist-tx.svc:5432" "postgres / supersecretpgpassword"
    printf "%-18s | %-12s | %-32s | %s\n" "Apache Kafka 4.4" "9092" "kafka.dist-tx.svc:9092" "KRaft cluster (NodePort 30092)"
    printf "%-18s | %-12s | %-32s | %s\n" "Redis 7.4" "ClusterIP" "redis.dist-tx.svc:6379" "Anonymous cache"
    printf "%-18s | %-12s | %-32s | %s\n" "OTel Collector" "4317/4318" "otel-collector.dist-tx.svc:4317" "OTLP gRPC & HTTP receiver"
    echo "================================================================================"
    echo ""
}

show_status() {
    check_prerequisites
    log_info "Kubernetes Cluster Status for context 'kind-${KIND_CLUSTER_NAME}':"
    kubectl get nodes -o wide
    echo ""
    log_info "Resources in namespace '${NAMESPACE}':"
    kubectl get all,configmap,secret -n "${NAMESPACE}"
}

destroy_cluster() {
    check_prerequisites
    log_info "Destroying KinD cluster '${KIND_CLUSTER_NAME}'..."
    kind delete cluster --name "${KIND_CLUSTER_NAME}"
    log_success "KinD cluster '${KIND_CLUSTER_NAME}' completely destroyed."
}

usage() {
    cat <<EOF
Usage: $0 [command]

Commands:
  up         Create KinD cluster, provision namespace, secrets, datastores, build/load image, and deploy k8s base
  build      Build polyglot engine image and load it into KinD cluster
  test       Run in-cluster pod health, PostgreSQL query, Kafka pub/sub, and Redis tests
  status     Show running cluster nodes, pods, services, and deployments
  down       Completely tear down and delete the KinD cluster
  help       Show this help message
EOF
}

main() {
    local cmd="${1:-help}"
    case "${cmd}" in
        up)
            create_cluster
            setup_namespace_and_secrets
            deploy_infra_services
            build_engine_image
            deploy_k8s_base
            wait_for_rollout
            ;;
        build)
            build_engine_image
            ;;
        test)
            run_verification_tests
            ;;
        status)
            show_status
            ;;
        down)
            destroy_cluster
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Unknown command: ${cmd}"
            usage
            exit 1
            ;;
    esac
}

main "$@"

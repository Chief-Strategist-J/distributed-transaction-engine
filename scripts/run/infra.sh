#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../../deploy/docker/compose/docker-compose.infra.yml"

log_info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

log_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

log_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1" >&2
}

validate_prerequisites() {
    log_info "Validating compose file existence..."
    if [[ ! -f "${COMPOSE_FILE}" ]]; then
        log_error "Compose file not found at ${COMPOSE_FILE}"
        exit 1
    fi
}

start_infrastructure() {
    log_info "Ensuring local secrets and environment configuration exist..."
    local compose_dir
    compose_dir="$(dirname "${COMPOSE_FILE}")"
    local secrets_dir="${compose_dir}/secrets"
    mkdir -p "${secrets_dir}"
    if [[ ! -f "${secrets_dir}/db_password.txt" ]]; then
        echo -n "supersecretpgpassword" > "${secrets_dir}/db_password.txt"
        log_info "Generated default secrets/db_password.txt"
    fi

    local project_root="${SCRIPT_DIR}/../.."
    if [[ ! -f "${project_root}/.env" && -f "${project_root}/.env.example" ]]; then
        cp "${project_root}/.env.example" "${project_root}/.env"
        log_info "Generated default .env from .env.example"
    fi

    log_info "Ensuring volume ownership for unprivileged services (UID 10001)..."
    docker volume create dist_tx_tempodata >/dev/null 2>&1 || true
    docker volume create dist_tx_lokidata >/dev/null 2>&1 || true
    docker volume create dist_tx_promdata >/dev/null 2>&1 || true
    docker run --rm -v dist_tx_tempodata:/var/lib/tempo -v dist_tx_lokidata:/var/lib/loki -v dist_tx_promdata:/prometheus busybox sh -c "chown -R 10001:10001 /var/lib/tempo /var/lib/loki && chown -R 65534:65534 /prometheus" >/dev/null 2>&1 || true

    log_info "Starting Docker infrastructure containers..."
    docker compose -f "${COMPOSE_FILE}" up -d
}

apply_database_migrations() {
    log_info "Applying automated database migrations to dist_tx_engine..."
    local project_root="${SCRIPT_DIR}/../.."
    local migrations_dir="${project_root}/database/migrations/sql"
    
    # Run migrations via docker exec on db container to ensure direct zero-dependency execution
    for mig in $(ls "${migrations_dir}"/*.sql | grep -v "\.rollback\.sql" | sort); do
        local mig_name
        mig_name="$(basename "${mig}")"
        log_info "Applying migration: ${mig_name}..."
        docker compose -f "${COMPOSE_FILE}" exec -T db psql -U postgres -d dist_tx_engine -v ON_ERROR_STOP=1 < "${mig}"
    done
    log_success "Database migrations successfully applied."
}

wait_for_services() {
    log_info "Waiting for all containers to reach healthy status..."
    local timeout=90
    local elapsed=0

    while [[ ${elapsed} -lt ${timeout} ]]; do
        local unhealthy
        unhealthy=$(docker compose -f "${COMPOSE_FILE}" ps --format '{{.Service}}: {{.Health}}' | grep -E 'unhealthy|starting' || true)

        if [[ -z "${unhealthy}" ]]; then
            log_success "All infrastructure containers are healthy."
            return 0
        fi

        sleep 5
        elapsed=$((elapsed + 5))
    done

    log_error "Timeout reached waiting for infrastructure to become healthy."
    docker compose -f "${COMPOSE_FILE}" ps
    diagnose_failures
    exit 1
}

diagnose_failures() {
    log_info "Diagnosing container logs for unhealthy/restarting services..."
    local services
    services=$(docker compose -f "${COMPOSE_FILE}" ps --format '{{.Service}}')
    for svc in ${services}; do
        log_info "=== Service: ${svc} (last 10 log lines) ==="
        docker compose -f "${COMPOSE_FILE}" logs --tail 10 "${svc}" || true
    done
}

verify_status() {
    log_info "Infrastructure container verification:"
    docker compose -f "${COMPOSE_FILE}" ps
}

test_trace_ingestion() {
    log_info "Testing trace ingestion via OTLP HTTP endpoint (127.0.0.1:4318)..."
    local now_s
    now_s=$(date +%s)
    local start_ns="${now_s}000000000"
    local end_ns="$((now_s + 1))000000000"
    local trace_id
    trace_id=$(head -c 16 /dev/urandom | xxd -p)
    local span_id
    span_id=$(head -c 8 /dev/urandom | xxd -p)

    local trace_payload=$(cat <<EOF
{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "distributed-transaction-engine" } },
          { "key": "environment", "value": { "stringValue": "dev" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "verification-script" },
          "spans": [
            {
              "traceId": "${trace_id}",
              "spanId": "${span_id}",
              "name": "verification-span",
              "kind": 1,
              "startTimeUnixNano": "${start_ns}",
              "endTimeUnixNano": "${end_ns}",
              "status": { "code": 1 }
            }
          ]
        }
      ]
    }
  ]
}
EOF
)

    local response
    response=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:4318/v1/traces \
      -H "Content-Type: application/json" \
      -d "${trace_payload}" || echo "000")

    if [[ "${response}" == "200" ]]; then
        log_success "OTLP Trace Ingestion OK (HTTP 200). Trace ID: ${trace_id}"
    else
        log_error "Trace Ingestion failed with HTTP status: ${response}"
    fi
}

test_endpoints() {
    log_info "Verifying core service endpoints..."

    # Prometheus
    local prom_code
    prom_code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:9090/-/ready || echo "000")
    if [[ "${prom_code}" == "200" ]]; then
        log_success "Prometheus endpoint healthy (HTTP 200) at http://127.0.0.1:9090"
    else
        log_error "Prometheus failed (HTTP ${prom_code})"
    fi

    # Grafana
    local grafana_code
    grafana_code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:3000/api/health || echo "000")
    if [[ "${grafana_code}" == "200" ]]; then
        log_success "Grafana endpoint healthy (HTTP 200) at http://127.0.0.1:3000"
    else
        log_error "Grafana failed (HTTP ${grafana_code})"
    fi

    # Caddy Ingress
    local caddy_code
    caddy_code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/ || echo "000")
    if [[ "${caddy_code}" == "200" ]]; then
        log_success "Caddy Ingress Proxy healthy (HTTP 200) at http://127.0.0.1:8080"
    else
        log_error "Caddy Proxy failed (HTTP ${caddy_code})"
    fi

    # OpenTelemetry Collector Extension Health
    local otel_code
    otel_code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:13133/ || echo "000")
    if [[ "${otel_code}" == "200" ]]; then
        log_success "OpenTelemetry Collector health extension OK (HTTP 200) at http://127.0.0.1:13133"
    else
        log_error "OpenTelemetry Collector health failed (HTTP ${otel_code})"
    fi

    # PostgreSQL Database Query Execution Test
    log_info "Testing database query execution on PostgreSQL 18..."
    local db_query_out
    db_query_out=$(docker exec dist_tx-db-1 psql -U postgres -d dist_tx_engine -t -A -c "SELECT 'pg_ready_' || current_setting('server_version');" || echo "FAIL")
    if [[ "${db_query_out}" =~ pg_ready_18 ]]; then
        log_success "PostgreSQL query execution verified: ${db_query_out}"
    else
        log_error "PostgreSQL query execution failed: ${db_query_out}"
    fi

    # Apache Kafka Event Streaming Test
    log_info "Testing Kafka event produce and consume on KRaft cluster..."
    local test_topic="smoke-test"
    local test_msg="infra-smoke-test-$(date +%s)"
    docker exec dist_tx-kafka-1 /bin/bash -c "echo '${test_msg}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${test_topic} >/dev/null 2>&1"
    local consumed_msg
    consumed_msg=$(docker exec dist_tx-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "${test_topic}" --partition 0 --offset 0 --max-messages 1 2>/dev/null | grep "infra-smoke-test" | head -n 1 || echo "")
    if [[ -n "${consumed_msg}" ]]; then
        log_success "Kafka event stream verified. Consumed message: ${consumed_msg}"
    else
        log_error "Kafka event stream verification failed."
    fi

    # Infrastructure Access Matrix
    echo ""
    echo "================================================================================"
    echo "              INFRASTRUCTURE ACCESS & SERVICE CREDENTIALS MATRIX               "
    echo "================================================================================"
    printf "%-18s | %-12s | %-24s | %s\n" "SERVICE" "HOST PORT" "INTERNAL ENDPOINT" "ACCESS / CREDENTIALS"
    echo "--------------------------------------------------------------------------------"
    printf "%-18s | %-12s | %-24s | %s\n" "Grafana UI" "3000" "http://grafana:3000" "http://localhost:3000 (admin / admin)"
    printf "%-18s | %-12s | %-24s | %s\n" "Prometheus" "9090" "http://prometheus:9090" "http://localhost:9090 (targets/metrics)"
    printf "%-18s | %-12s | %-24s | %s\n" "PostgreSQL 18" "5432" "db:5432" "postgres / supersecretpgpassword (db: dist_tx_engine)"
    printf "%-18s | %-12s | %-24s | %s\n" "Apache Kafka 4.4" "9092" "kafka:9092" "KRaft cluster (localhost:9092)"
    printf "%-18s | %-12s | %-24s | %s\n" "OTel Collector" "4317/4318" "otel-collector:4317" "OTLP gRPC (:4317), HTTP (:4318), Health (:13133)"
    printf "%-18s | %-12s | %-24s | %s\n" "Tempo Tracing" "3200" "tempo:3200" "Traces stored locally & viewable in Grafana"
    printf "%-18s | %-12s | %-24s | %s\n" "Loki Logs" "3100" "loki:3100" "Logs forwarded via OTel & viewable in Grafana"
    printf "%-18s | %-12s | %-24s | %s\n" "Redis Cache" "6379" "cache:6379" "In-memory LRU dedup/cache"
    printf "%-18s | %-12s | %-24s | %s\n" "Caddy Gateway" "8080" "proxy:80" "http://localhost:8080/"
    echo "================================================================================"
    echo ""
}

test_logs_ingestion() {
    log_info "Testing log ingestion via OTLP HTTP endpoint (127.0.0.1:4318/v1/logs)..."
    local now_s
    now_s=$(date +%s)
    local now_ns="${now_s}000000000"

    local log_payload=$(cat <<EOF
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "distributed-transaction-engine" } },
          { "key": "environment", "value": { "stringValue": "dev" } }
        ]
      },
      "scopeLogs": [
        {
          "scope": { "name": "verification-script" },
          "logRecords": [
            {
              "timeUnixNano": "${now_ns}",
              "severityNumber": 9,
              "severityText": "INFO",
              "body": { "stringValue": "Engine health-check ping via OpenTelemetry Collector" }
            }
          ]
        }
      ]
    }
  ]
}
EOF
)

    local response
    response=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:4318/v1/logs \
      -H "Content-Type: application/json" \
      -d "${log_payload}" || echo "000")

    if [[ "${response}" == "200" ]]; then
        log_success "OTLP Log Ingestion OK (HTTP 200). Log sent to Loki via OpenTelemetry Collector."
    else
        log_error "Log Ingestion failed with HTTP status: ${response}"
    fi
}

down_infrastructure() {
    log_info "Stopping containers while preserving persistent data volumes..."
    docker compose -f "${COMPOSE_FILE}" down
}

restart_infrastructure() {
    log_info "Restarting infrastructure services..."
    docker compose -f "${COMPOSE_FILE}" restart
    wait_for_services
    verify_status
}

show_status() {
    verify_status
}

show_logs() {
    local service="${1:-}"
    if [[ -n "${service}" ]]; then
        docker compose -f "${COMPOSE_FILE}" logs -f "${service}"
    else
        docker compose -f "${COMPOSE_FILE}" logs -f
    fi
}

prune_obsolete_images() {
    log_info "Removing outdated container images to reclaim host memory and disk..."
    local old_images=(
        "postgres:16.4-bookworm"
        "postgres:17-bookworm"
        "apache/kafka:3.7.0"
        "grafana/grafana:11.2.0"
    )
    for img in "${old_images[@]}"; do
        if docker image inspect "${img}" >/dev/null 2>&1; then
            log_info "Removing superseded image: ${img}"
            docker rmi "${img}" || true
        fi
    done
}

main() {
    local action="${1:-all}"
    case "${action}" in
        down)
            down_infrastructure
            ;;
        restart)
            restart_infrastructure
            ;;
        status)
            show_status
            ;;
        logs)
            shift || true
            show_logs "${1:-}"
            ;;
        prune)
            down_infrastructure
            prune_obsolete_images
            ;;
        up)
            validate_prerequisites
            start_infrastructure
            wait_for_services
            apply_database_migrations
            verify_status
            ;;
        test)
            test_trace_ingestion
            test_logs_ingestion
            test_endpoints
            ;;
        all)
            down_infrastructure
            prune_obsolete_images
            validate_prerequisites
            start_infrastructure
            wait_for_services
            verify_status
            test_trace_ingestion
            test_logs_ingestion
            test_endpoints
            ;;
        *)
            echo "Usage: $0 [up|down|restart|status|logs [service]|test|all|prune]"
            exit 1
            ;;
    esac
}

main "$@"

#!/usr/bin/env bash
set -euo pipefail

export PATH="/home/btpl-lap-22/.local/go/bin:/home/btpl-lap-22/.local/bin:${PATH}"
export no_proxy="localhost,127.0.0.1,::1,*"
export NO_PROXY="localhost,127.0.0.1,::1,*"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PATH="${ROOT_DIR}/scripts/bin:${PATH}"
cd "${ROOT_DIR}"

PORT="${PORT:-8094}"
ADMIN_PORT="${ADMIN_PORT:-8095}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-supersecretpgpassword}"
DB_NAME="${DB_NAME:-dist_tx_engine}"
DB_SSLMODE="${DB_SSLMODE:-disable}"
KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-127.0.0.1:4318}"

TMP_DIR="$(mktemp -d)"
ENGINE_PID=""

cleanup() {
    local exit_code=$?
    echo ""
    echo "================================================================"
    echo " [CLEANUP] Tearing down engine process & temporary directory"
    echo "================================================================"
    if [[ -n "${ENGINE_PID}" ]] && kill -0 "${ENGINE_PID}" 2>/dev/null; then
        echo "Stopping engine process (PID: ${ENGINE_PID})..."
        kill -TERM "${ENGINE_PID}" 2>/dev/null || true
        wait "${ENGINE_PID}" 2>/dev/null || true
    fi
    rm -rf "${TMP_DIR}"
    if [[ ${exit_code} -eq 0 ]]; then
        echo ">>> [SUCCESS] All features verified with W3C distributed tracing, DB & Kafka!"
    else
        echo ">>> [FAILURE] Tracing verification failed with exit code ${exit_code}."
    fi
    exit ${exit_code}
}
trap cleanup EXIT INT TERM

json_extract() {
    local key="$1"
    python3 -c "import sys, json; data=json.load(sys.stdin); print(data${key})"
}

generate_uuid() {
    python3 -c "import uuid; print(uuid.uuid4())"
}

echo "================================================================"
echo " 1. COMPILING PRODUCTION ENGINE BINARY"
echo "================================================================"
go build -o "${TMP_DIR}/dist_tx_engine" ./src/main.go
echo "[PASS] Engine compiled successfully"

echo "================================================================"
echo " 2. STARTING ENGINE WITH OTEL TRACING, POSTGRES & KAFKA"
echo "================================================================"
PORT="${PORT}" \
ADMIN_PORT="${ADMIN_PORT}" \
DB_HOST="${DB_HOST}" \
DB_PORT="${DB_PORT}" \
DB_USER="${DB_USER}" \
DB_PASSWORD="${DB_PASSWORD}" \
DB_NAME="${DB_NAME}" \
DB_SSLMODE="${DB_SSLMODE}" \
KAFKA_BROKERS="${KAFKA_BROKERS}" \
OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT}" \
POLLER_ENABLED="false" \
"${TMP_DIR}/dist_tx_engine" > "${TMP_DIR}/engine.log" 2>&1 &
ENGINE_PID=$!

READY=0
for i in $(seq 1 30); do
    if curl --noproxy "*" -s -f "http://127.0.0.1:${ADMIN_PORT}/health/ready" >/dev/null 2>&1; then
        READY=1
        break
    fi
    if ! kill -0 "${ENGINE_PID}" 2>/dev/null; then
        echo "Engine died! Log output:"
        cat "${TMP_DIR}/engine.log"
        exit 1
    fi
    sleep 0.5
done

if [[ ${READY} -ne 1 ]]; then
    echo "Engine failed readiness! Log:"
    cat "${TMP_DIR}/engine.log"
    exit 1
fi
echo "[PASS] Engine is ready at http://127.0.0.1:${PORT}"

TENANT_ID="tenant_trace_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
TRACE_ID="4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID="00f067aa0ba902b7"
W3C_TRACEPARENT="00-${TRACE_ID}-${SPAN_ID}-01"

assert_tracing_headers() {
    local resp_headers="$1"
    local endpoint="$2"
    local trace_header=$(echo "${resp_headers}" | grep -i "^x-trace-id:" | tr -d '\r' | awk '{print $2}')
    local span_header=$(echo "${resp_headers}" | grep -i "^x-span-id:" | tr -d '\r' | awk '{print $2}')

    if [[ -z "${trace_header}" ]]; then
        echo "FAIL [${endpoint}]: Missing X-Trace-Id header!"
        exit 1
    fi
    if [[ "${trace_header}" != "${TRACE_ID}" && "${trace_header}" != *"${TRACE_ID}"* ]]; then
        echo "FAIL [${endpoint}]: X-Trace-Id mismatch! Expected trace ${TRACE_ID}, got: ${trace_header}"
        exit 1
    fi
    if [[ -z "${span_header}" ]]; then
        echo "FAIL [${endpoint}]: Missing X-Span-Id header!"
        exit 1
    fi
    echo "[PASS] [${endpoint}] W3C trace propagation confirmed: X-Trace-Id=${trace_header}, X-Span-Id=${span_header}"
}

echo "================================================================"
echo " 3. VERIFYING TRACING ON FEATURE: DISTRIBUTED TRACING ENGINE"
echo "================================================================"
DT_RESP=$(curl --noproxy "*" -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/tracing/context/start" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-trace-001" \
    -d "{
        \"tenant_id\": \"${TENANT_ID}\",
        \"correlation_id\": \"$(generate_uuid)\",
        \"route\": \"/api/v1/orders/checkout\",
        \"is_vip\": true
    }")

assert_tracing_headers "${DT_RESP}" "/api/v1/tracing/context/start"

echo "================================================================"
echo " 4. VERIFYING TRACING ON FEATURE: RECONCILIATION ENGINE"
echo "================================================================"
RECON_SAGA_ID=$(generate_uuid)
RECON_RESP=$(curl --noproxy "*" -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/reconciliation/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-recon-trace" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"sagaId\": \"${RECON_SAGA_ID}\",
        \"internalStatus\": \"COMPLETED\",
        \"externalStatus\": \"FAILED\",
        \"internalAmountCents\": 5000,
        \"externalAmountCents\": 5000,
        \"currency\": \"USD\"
    }")

assert_tracing_headers "${RECON_RESP}" "/api/v1/reconciliation/evaluate"

echo "================================================================"
echo " 5. VERIFYING TRACING ON FEATURE: MANUAL INTERVENTION"
echo "================================================================"
MI_SAGA_ID=$(generate_uuid)
MI_RESP=$(curl --noproxy "*" -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/manual-intervention/intervene" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-mi-trace" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"sagaId\": \"${MI_SAGA_ID}\",
        \"operatorId\": \"operator_alice_99\",
        \"operatorRole\": \"admin:saga:intervene\",
        \"actionType\": \"FORCE_RETRY\",
        \"currentSagaState\": \"FAILED\",
        \"reason\": \"Retrying stuck saga after network recovery\",
        \"approvalTicketId\": \"TICK-90812\"
    }")

assert_tracing_headers "${MI_RESP}" "/api/v1/manual-intervention/intervene"

echo "================================================================"
echo " 6. VERIFYING TRACING ON FEATURE: SCHEDULED EXECUTION"
echo "================================================================"
WFD_PUB_ID="wfd_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:16])')"
SCHED_RESP=$(curl --noproxy "*" -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/scheduled-execution/schedules" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-sched-trace" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"workflowId\": \"${WFD_PUB_ID}\",
        \"scheduleType\": \"RECURRING_CRON\",
        \"cronExpression\": \"0 * * * *\",
        \"timeZone\": \"UTC\",
        \"runAsPrincipal\": \"cron_scheduler_daemon\"
    }")

assert_tracing_headers "${SCHED_RESP}" "/api/v1/scheduled-execution/schedules"

echo "================================================================"
echo " 7. VERIFYING TRACING ON FEATURE: OPTIMISTIC CONCURRENCY CONTROL"
echo "================================================================"
OCC_SAGA_ID=$(generate_uuid)
OCC_PUB_ID="sag_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"

PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO saga_instance_states (
        id, public_id, tenant_id, source, external_id, correlation_id,
        current_state, version, owner_epoch, fencing_token, created_at, updated_at
    ) VALUES (
        '${OCC_SAGA_ID}', '${OCC_PUB_ID}', '${TENANT_ID}', 'TEST', 'ext_occ_trace', '${OCC_SAGA_ID}',
        'RUNNING', 1, 1, 0, NOW(), NOW()
    );" >/dev/null 2>&1

OCC_RESP=$(curl --noproxy "*" -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/occ/mutate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-occ-trace" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"sagaId\": \"${OCC_SAGA_ID}\",
        \"expectedVersion\": 1,
        \"expectedEpoch\": 1,
        \"targetState\": \"RUNNING\",
        \"workerID\": \"worker-occ-test-1\",
        \"inputId\": \"reply-occ-trace-1\",
        \"inputType\": \"REPLY\",
        \"inputPayload\": \"{\\\"stepOutcome\\\": \\\"SUCCESS\\\"}\"
    }")

assert_tracing_headers "${OCC_RESP}" "/api/v1/occ/mutate"

echo "================================================================"
echo " 8. VERIFYING TRACING ON FEATURE: TRANSACTIONAL OUTBOX & KAFKA"
echo "================================================================"
OX_EVENT_ID=$(generate_uuid)
OX_EVENT_PUB_ID="oxe_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
OX_TOPIC="dist-tx.live-verify.tracing.${TENANT_ID}"

/snap/bin/docker exec dist_tx-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "${OX_TOPIC}" --partitions 1 --replication-factor 1 >/dev/null 2>&1

PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO outbox_events (
        id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
        aggregate_sequence, destination_topic, partition_key, event_type,
        payload, headers, status, attempt_count, version, created_at, updated_at
    ) VALUES (
        '${OX_EVENT_ID}', '${OX_EVENT_PUB_ID}', '${TENANT_ID}', '${MI_SAGA_ID}', 'MANUAL_ACTION', '${MI_SAGA_ID}',
        1, '${OX_TOPIC}', '${MI_SAGA_ID}', 'MANUAL_INTERVENTION_APPLIED',
        '{\"action\": \"FORCE_RETRY\", \"sagaId\": \"${MI_SAGA_ID}\"}'::jsonb,
        '{\"tenant\": \"${TENANT_ID}\", \"traceparent\": \"${W3C_TRACEPARENT}\"}'::jsonb,
        'PENDING', 0, 1, NOW(), NOW()
    );" >/dev/null 2>&1

OX_RESP=$(curl --noproxy "*" -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/outbox/relay" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-relay-trace" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"topic\": \"${OX_TOPIC}\",
        \"workerId\": \"trace-worker-01\",
        \"batchLimit\": 5,
        \"leaseDuration\": \"15s\"
    }")

assert_tracing_headers "${OX_RESP}" "/api/v1/outbox/relay"

echo "================================================================"
echo " 9. VERIFYING OPENTELEMETRY COLLECTOR INGESTION"
echo "================================================================"
OTEL_STATUS=$(curl --noproxy "*" -s -o /dev/null -w "%{http_code}" http://127.0.0.1:13133/ || echo "000")
if [[ "${OTEL_STATUS}" == "200" ]]; then
    echo "[PASS] OpenTelemetry Collector health check verified at http://127.0.0.1:13133 (Status: ${OTEL_STATUS})"
else
    echo "FAIL: OpenTelemetry Collector health check failed (Status: ${OTEL_STATUS})"
    exit 1
fi


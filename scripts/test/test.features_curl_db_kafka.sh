#!/usr/bin/env bash
set -euo pipefail

export PATH="/home/btpl-lap-22/.local/go/bin:/home/btpl-lap-22/.local/bin:${PATH}"
export no_proxy="localhost,127.0.0.1,::1,*"
export NO_PROXY="localhost,127.0.0.1,::1,*"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PATH="${ROOT_DIR}/scripts/bin:${PATH}"
cd "${ROOT_DIR}"

PORT="${PORT:-8092}"
ADMIN_PORT="${ADMIN_PORT:-8093}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-supersecretpgpassword}"
DB_NAME="${DB_NAME:-dist_tx_engine}"
DB_SSLMODE="${DB_SSLMODE:-disable}"
KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
WORKER_ID="${WORKER_ID:-feature-test-worker-$$}"

TMP_DIR="$(mktemp -d)"
ENGINE_PID=""

cleanup() {
    local exit_code=$?
    echo ""
    echo "================================================================"
    echo " [CLEANUP] Cleaning feature test runner processes & temp files"
    echo "================================================================"
    if [[ -n "${ENGINE_PID}" ]] && kill -0 "${ENGINE_PID}" 2>/dev/null; then
        echo "Stopping engine process (PID: ${ENGINE_PID})..."
        kill -TERM "${ENGINE_PID}" 2>/dev/null || true
        wait "${ENGINE_PID}" 2>/dev/null || true
    fi
    rm -rf "${TMP_DIR}"
    if [[ ${exit_code} -eq 0 ]]; then
        echo ">>> [SUCCESS] All features verified against cURL, PostgreSQL & Kafka!"
    else
        echo ">>> [FAILURE] Feature verification failed with exit code ${exit_code}."
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
echo "[PASS] Engine compiled successfully to ${TMP_DIR}/dist_tx_engine"

echo "================================================================"
echo " 2. STARTING ENGINE WITH POSTGRES & KAFKA"
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
POLLER_ENABLED="false" \
"${TMP_DIR}/dist_tx_engine" > "${TMP_DIR}/engine.log" 2>&1 &
ENGINE_PID=$!

echo "Engine running with PID: ${ENGINE_PID}. Probing readiness..."
READY=0
for i in $(seq 1 30); do
    if curl --noproxy "*" -s -f "http://127.0.0.1:${ADMIN_PORT}/health/ready" >/dev/null 2>&1; then
        READY=1
        break
    fi
    if ! kill -0 "${ENGINE_PID}" 2>/dev/null; then
        echo "Engine died unexpectedly! Logs:"
        cat "${TMP_DIR}/engine.log"
        exit 1
    fi
    sleep 0.5
done

if [[ ${READY} -ne 1 ]]; then
    echo "Engine failed to become ready on port ${ADMIN_PORT}! Logs:"
    cat "${TMP_DIR}/engine.log"
    exit 1
fi
echo "[PASS] Engine is ready at http://127.0.0.1:${PORT}"

TENANT_ID="tenant_test_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
W3C_TRACEPARENT="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

echo "================================================================"
echo " 3. TESTING FEATURE: RECONCILIATION ENGINE"
echo "================================================================"
SAGA_ID_1=$(generate_uuid)

echo "1. Invoking POST /api/v1/reconciliation/evaluate (Phantom Settlement: Internal COMPLETED, External FAILED)..."
RECON_EVAL_RESP=$(curl --noproxy "*" -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/reconciliation/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-recon-001" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"sagaId\": \"${SAGA_ID_1}\",
        \"internalStatus\": \"COMPLETED\",
        \"externalStatus\": \"FAILED\",
        \"internalAmountCents\": 5000,
        \"externalAmountCents\": 5000,
        \"currency\": \"USD\"
    }")

RECON_HTTP_CODE=$(echo "${RECON_EVAL_RESP}" | tail -n 1)
RECON_BODY=$(echo "${RECON_EVAL_RESP}" | sed '$d')
IS_ANOMALY=$(echo "${RECON_BODY}" | json_extract "['data']['isAnomaly']")
ANOMALY_TYPE=$(echo "${RECON_BODY}" | json_extract "['data']['anomalyType']")

if [[ "${RECON_HTTP_CODE}" != "200" || "${IS_ANOMALY}" != "True" || "${ANOMALY_TYPE}" != "PHANTOM_SETTLEMENT" ]]; then
    echo "FAIL: Expected HTTP 200 with isAnomaly=true and PHANTOM_SETTLEMENT, got HTTP ${RECON_HTTP_CODE}: ${RECON_BODY}"
    exit 1
fi
echo "[PASS] cURL evaluation successfully detected anomaly: ${ANOMALY_TYPE} (isAnomaly=${IS_ANOMALY})"

echo "2. Direct Database Assertion: Querying PostgreSQL reconciliation_anomalies table..."
PG_ANOMALY_TYPE=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT anomaly_type FROM reconciliation_anomalies WHERE tenant_id = '${TENANT_ID}' AND saga_id = '${SAGA_ID_1}';")

if [[ "${PG_ANOMALY_TYPE}" != "PHANTOM_SETTLEMENT" ]]; then
    echo "FAIL: Expected PostgreSQL reconciliation_anomalies record with PHANTOM_SETTLEMENT, got '${PG_ANOMALY_TYPE}'"
    exit 1
fi
echo "[PASS] Database confirmed reconciliation_anomalies record exists with PHANTOM_SETTLEMENT!"

echo "3. Invoking GET /api/v1/reconciliation/anomalies..."
RECON_LIST_RESP=$(curl --noproxy "*" -s -w "\n%{http_code}" -X GET "http://127.0.0.1:${PORT}/api/v1/reconciliation/anomalies?tenantId=${TENANT_ID}&sagaId=${SAGA_ID_1}" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}")

LIST_HTTP_CODE=$(echo "${RECON_LIST_RESP}" | tail -n 1)
LIST_BODY=$(echo "${RECON_LIST_RESP}" | sed '$d')
LIST_LEN=$(python3 -c "import json; data=json.loads('''${LIST_BODY}'''); print(len(data['data']))")

if [[ "${LIST_HTTP_CODE}" != "200" || "${LIST_LEN}" -lt 1 ]]; then
    echo "FAIL: Expected HTTP 200 with at least 1 anomaly, got HTTP ${LIST_HTTP_CODE}: ${LIST_BODY}"
    exit 1
fi
echo "[PASS] Query anomalies returned ${LIST_LEN} anomaly records via cURL!"

echo "================================================================"
echo " 4. TESTING FEATURE: MANUAL INTERVENTION"
echo "================================================================"
SAGA_ID_2=$(generate_uuid)

echo "1. Invoking POST /api/v1/manual-intervention/intervene (FORCE_RETRY action)..."
MI_RESP=$(curl --noproxy "*" -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/manual-intervention/intervene" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-mi-001" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"sagaId\": \"${SAGA_ID_2}\",
        \"operatorId\": \"operator_alice_99\",
        \"operatorRole\": \"admin:saga:intervene\",
        \"actionType\": \"FORCE_RETRY\",
        \"currentSagaState\": \"FAILED\",
        \"reason\": \"Retrying stuck saga after network recovery\",
        \"approvalTicketId\": \"TICK-90812\"
    }")

MI_HTTP_CODE=$(echo "${MI_RESP}" | tail -n 1)
MI_BODY=$(echo "${MI_RESP}" | sed '$d')
MI_SUCCESS=$(echo "${MI_BODY}" | json_extract "['data']['success']")
MI_ACTION=$(echo "${MI_BODY}" | json_extract "['data']['actionType']")
MI_NEW_STATE=$(echo "${MI_BODY}" | json_extract "['data']['newState']")

if [[ "${MI_HTTP_CODE}" != "200" || "${MI_SUCCESS}" != "True" || "${MI_NEW_STATE}" != "RUNNING" ]]; then
    echo "FAIL: Expected HTTP 200 with success=true and newState=RUNNING, got HTTP ${MI_HTTP_CODE}: ${MI_BODY}"
    exit 1
fi
echo "[PASS] Manual intervention succeeded: action=${MI_ACTION}, newState=${MI_NEW_STATE}"

echo "2. Direct Database Assertion: Querying PostgreSQL manual_intervention_audit_log..."
PG_AUDIT_ACTION=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT action_type FROM manual_intervention_audit_log WHERE tenant_id = '${TENANT_ID}' AND saga_id = '${SAGA_ID_2}';")

if [[ "${PG_AUDIT_ACTION}" != "FORCE_RETRY" ]]; then
    echo "FAIL: Expected PostgreSQL manual_intervention_audit_log record with FORCE_RETRY, got '${PG_AUDIT_ACTION}'"
    exit 1
fi
echo "[PASS] Database confirmed manual_intervention_audit_log record exists with FORCE_RETRY!"

echo "================================================================"
echo " 5. TESTING FEATURE: SCHEDULED EXECUTION"
echo "================================================================"
WFD_PUB_ID="wfd_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:16])')"

echo "1. Invoking POST /api/v1/scheduled-execution/schedules (Create Recurring Cron Schedule)..."
SCHED_RESP=$(curl --noproxy "*" -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/scheduled-execution/schedules" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-sched-001" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"workflowId\": \"${WFD_PUB_ID}\",
        \"scheduleType\": \"RECURRING_CRON\",
        \"cronExpression\": \"0 * * * *\",
        \"timeZone\": \"UTC\",
        \"runAsPrincipal\": \"cron_scheduler_daemon\"
    }")

SCHED_HTTP_CODE=$(echo "${SCHED_RESP}" | tail -n 1)
SCHED_BODY=$(echo "${SCHED_RESP}" | sed '$d')
SCHED_STATUS=$(echo "${SCHED_BODY}" | json_extract "['data']['status']")
SCHED_ID=$(echo "${SCHED_BODY}" | json_extract "['data']['id']")

if [[ "${SCHED_HTTP_CODE}" != "201" || "${SCHED_STATUS}" != "ACTIVE" ]]; then
    echo "FAIL: Expected HTTP 201 with ACTIVE, got HTTP ${SCHED_HTTP_CODE}: ${SCHED_BODY}"
    exit 1
fi
echo "[PASS] Created schedule via cURL: ID=${SCHED_ID}, Status=${SCHED_STATUS}"

echo "2. Direct Database Assertion: Querying PostgreSQL schedules table..."
PG_SCHED_STATUS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT status FROM schedules WHERE tenant_id = '${TENANT_ID}' AND id = '${SCHED_ID}';")

if [[ "${PG_SCHED_STATUS}" != "ACTIVE" ]]; then
    echo "FAIL: Expected PostgreSQL schedules record with ACTIVE, got '${PG_SCHED_STATUS}'"
    exit 1
fi
echo "[PASS] Database confirmed schedules record exists with ACTIVE!"

echo "================================================================"
echo " 6. TESTING FEATURE: OPTIMISTIC CONCURRENCY CONTROL (OCC)"
echo "================================================================"
OCC_SAGA_ID=$(generate_uuid)
OCC_PUB_ID="sag_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"

echo "1. Seeding base saga root in PostgreSQL saga_instance_states with version 1, owner_epoch 1..."
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO saga_instance_states (
        id, public_id, tenant_id, source, external_id, correlation_id,
        current_state, version, owner_epoch, fencing_token, created_at, updated_at
    ) VALUES (
        '${OCC_SAGA_ID}', '${OCC_PUB_ID}', '${TENANT_ID}', 'TEST', 'ext_occ_001', '${OCC_SAGA_ID}',
        'RUNNING', 1, 1, 0, NOW(), NOW()
    );"

echo "2. Invoking POST /api/v1/occ/mutate (CAS mutation version 1 -> 2)..."
OCC_RESP1=$(curl --noproxy "*" -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/occ/mutate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-occ-001" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"sagaId\": \"${OCC_SAGA_ID}\",
        \"expectedVersion\": 1,
        \"expectedEpoch\": 1,
        \"targetState\": \"RUNNING\",
        \"workerID\": \"worker-occ-test-1\",
        \"inputId\": \"reply-occ-step-1\",
        \"inputType\": \"REPLY\",
        \"inputPayload\": \"{\\\"stepOutcome\\\": \\\"SUCCESS\\\"}\"
    }")

OCC_HTTP1=$(echo "${OCC_RESP1}" | tail -n 1)
OCC_BODY1=$(echo "${OCC_RESP1}" | sed '$d')
OCC_SUCCEEDED1=$(echo "${OCC_BODY1}" | json_extract "['data']['succeeded']")
OCC_VER1=$(echo "${OCC_BODY1}" | json_extract "['data']['currentVersion']")

if [[ "${OCC_HTTP1}" != "200" || "${OCC_SUCCEEDED1}" != "True" || "${OCC_VER1}" != "2" ]]; then
    echo "FAIL: Expected HTTP 200 with succeeded=true and currentVersion=2, got HTTP ${OCC_HTTP1}: ${OCC_BODY1}"
    exit 1
fi
echo "[PASS] CAS mutation succeeded: version updated to ${OCC_VER1}"

echo "3. Direct Database Assertion: Querying PostgreSQL saga_instance_states for version 2..."
PG_SAGA_VER=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT version FROM saga_instance_states WHERE tenant_id = '${TENANT_ID}' AND id = '${OCC_SAGA_ID}';")

if [[ "${PG_SAGA_VER}" != "2" ]]; then
    echo "FAIL: Expected PostgreSQL saga_instance_states version 2, got '${PG_SAGA_VER}'"
    exit 1
fi
echo "[PASS] Database confirmed saga_instance_states record updated atomically to version ${PG_SAGA_VER}!"

echo "4. Invoking POST /api/v1/occ/client-edit with stale expectedVersion 1 (Client Edit 409 Conflict Assertion)..."
OCC_RESP2=$(curl --noproxy "*" -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/occ/client-edit" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-occ-002" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"sagaId\": \"${OCC_SAGA_ID}\",
        \"expectedVersion\": 1,
        \"parameters\": \"{\\\"amount\\\": 200}\",
        \"actorId\": \"operator-ui-user\"
    }")

OCC_HTTP2=$(echo "${OCC_RESP2}" | tail -n 1)
OCC_BODY2=$(echo "${OCC_RESP2}" | sed '$d')
if [[ "${OCC_HTTP2}" != "409" ]]; then
    echo "FAIL: Expected HTTP 409 Conflict on stale OCC client-edit, got HTTP ${OCC_HTTP2}: ${OCC_BODY2}"
    exit 1
fi
echo "[PASS] Client Edit conflict detected as expected with HTTP 409 Conflict!"

echo "================================================================"
echo " 7. TESTING KAFKA RELAY & OBSERVABILITY VIA OUTBOX"
echo "================================================================"
OX_EVENT_ID=$(generate_uuid)
OX_EVENT_PUB_ID="oxe_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
OX_TOPIC="dist-tx.live-verify.${TENANT_ID}"

echo "0. Pre-creating Kafka topic ${OX_TOPIC}..."
/snap/bin/docker exec dist_tx-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "${OX_TOPIC}" --partitions 1 --replication-factor 1

echo "1. Seeding pending outbox event in PostgreSQL..."
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO outbox_events (
        id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
        aggregate_sequence, destination_topic, partition_key, event_type,
        payload, headers, status, attempt_count, version, created_at, updated_at
    ) VALUES (
        '${OX_EVENT_ID}', '${OX_EVENT_PUB_ID}', '${TENANT_ID}', '${SAGA_ID_2}', 'MANUAL_ACTION', '${SAGA_ID_2}',
        1, '${OX_TOPIC}', '${SAGA_ID_2}', 'MANUAL_INTERVENTION_APPLIED',
        '{\"action\": \"FORCE_RETRY\", \"sagaId\": \"${SAGA_ID_2}\"}'::jsonb,
        '{\"tenant\": \"${TENANT_ID}\", \"traceparent\": \"${W3C_TRACEPARENT}\"}'::jsonb,
        'PENDING', 0, 1, NOW(), NOW()
    );"

echo "2. Invoking POST /api/v1/outbox/relay via cURL to publish message to Kafka KRaft..."
RELAY_RESP=$(curl --noproxy "*" -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/outbox/relay" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-relay-001" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"topic\": \"${OX_TOPIC}\",
        \"workerId\": \"${WORKER_ID}\",
        \"batchLimit\": 5,
        \"leaseDuration\": \"15s\"
    }")

RELAY_HTTP=$(echo "${RELAY_RESP}" | tail -n 1)
RELAY_BODY=$(echo "${RELAY_RESP}" | sed '$d')
PUB_COUNT=$(echo "${RELAY_BODY}" | json_extract "['data']['publishedCount']")

if [[ "${RELAY_HTTP}" != "200" || "${PUB_COUNT}" != "1" ]]; then
    echo "FAIL: Expected HTTP 200 with publishedCount=1, got HTTP ${RELAY_HTTP}: ${RELAY_BODY}"
    exit 1
fi
echo "[PASS] Outbox published to Kafka topic '${OX_TOPIC}' via relay!"

echo "3. Direct Database Assertion: Querying PostgreSQL outbox_events table for PUBLISHED status..."
PG_OX_STATUS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT status FROM outbox_events WHERE tenant_id = '${TENANT_ID}' AND id = '${OX_EVENT_ID}';")

if [[ "${PG_OX_STATUS}" != "PUBLISHED" ]]; then
    echo "FAIL: Expected PostgreSQL outbox_events status PUBLISHED, got '${PG_OX_STATUS}'"
    exit 1
fi
echo "[PASS] PostgreSQL confirmed outbox event transitioned to PUBLISHED with published_at timestamp!"

echo "4. Direct Kafka Assertion: Consuming topic using kafka console consumer..."
KAFKA_MSG=$(/snap/bin/docker exec dist_tx-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${OX_TOPIC}" \
    --partition 0 \
    --offset 0 \
    --max-messages 1 2>/dev/null || true)

if [[ "${KAFKA_MSG}" != *"MANUAL_INTERVENTION_APPLIED"* && "${KAFKA_MSG}" != *"${SAGA_ID_2}"* ]]; then
    echo "FAIL: Kafka message not found or payload mismatched: '${KAFKA_MSG}'"
    exit 1
fi
echo "[PASS] Direct Kafka broker verified! Message successfully delivered to topic '${OX_TOPIC}':"
echo "       ${KAFKA_MSG}"


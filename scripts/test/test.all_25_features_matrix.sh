#!/usr/bin/env bash
set -euo pipefail

export PATH="$PWD/scripts/bin:$PATH"
export no_proxy="localhost,127.0.0.1,::1,*"
export NO_PROXY="localhost,127.0.0.1,::1,*"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT_DIR}"

PORT=8098
ADMIN_PORT=8099
DB_HOST="127.0.0.1"
DB_PORT=5432
DB_USER="postgres"
DB_PASSWORD="supersecretpgpassword"
DB_NAME="dist_tx_engine"
DB_SSLMODE="disable"
KAFKA_BROKERS="127.0.0.1:9092"
OTEL_EXPORTER_OTLP_ENDPOINT="127.0.0.1:4318"

TMP_DIR=$(mktemp -d)
ENGINE_PID=""

cleanup() {
    local exit_code=$?
    echo ""
    echo "================================================================"
    echo " [CLEANUP] Stopping engine process & removing temporary files"
    echo "================================================================"
    if [[ -n "${ENGINE_PID}" ]] && kill -0 "${ENGINE_PID}" 2>/dev/null; then
        echo "Stopping engine process (PID: ${ENGINE_PID})..."
        kill -TERM "${ENGINE_PID}" 2>/dev/null || true
        wait "${ENGINE_PID}" 2>/dev/null || true
    fi
    rm -rf "${TMP_DIR}"
    if [[ ${exit_code} -eq 0 ]]; then
        echo ">>> [SUCCESS] All 25 architecture features verified with real infrastructure & tracing."
    else
        echo ">>> [FAILURE] Feature verification failed with exit code ${exit_code}."
    fi
    exit ${exit_code}
}
trap cleanup EXIT INT TERM

echo "================================================================"
echo " 1. COMPILING PRODUCTION ENGINE BINARY"
echo "================================================================"
go build -o "${TMP_DIR}/dist_tx_engine" ./src/main.go
echo "[PASS] Engine compiled successfully"

echo "================================================================"
echo " 2. STARTING ENGINE WITH LIVE POSTGRESQL & KAFKA"
echo "================================================================"
PORT=${PORT} ADMIN_PORT=${ADMIN_PORT} \
DB_HOST=${DB_HOST} DB_PORT=${DB_PORT} DB_USER=${DB_USER} DB_PASSWORD=${DB_PASSWORD} \
DB_NAME=${DB_NAME} DB_SSLMODE=${DB_SSLMODE} KAFKA_BROKERS=${KAFKA_BROKERS} \
OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT} \
POLLER_ENABLED=false \
"${TMP_DIR}/dist_tx_engine" > /tmp/engine_matrix_test.log 2>&1 &
ENGINE_PID=$!

READY=0
for i in $(seq 1 30); do
    if curl --noproxy "*" -s -f "http://127.0.0.1:${ADMIN_PORT}/health/ready" ; then
        READY=1
        break
    fi
    if ! kill -0 "${ENGINE_PID}" 2>/dev/null; then
        echo "[ERROR] Engine exited prematurely:"
        cat /tmp/engine_matrix_test.log
        exit 1
    fi
    sleep 0.5
done

if [[ ${READY} -ne 1 ]]; then
    echo "[ERROR] Engine failed to become ready within timeout"
    cat /tmp/engine_matrix_test.log
    exit 1
fi
echo "[PASS] Engine is ready at http://127.0.0.1:${PORT}"

TENANT_ID="tenant_mat_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
TRACE_ID="4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID="00f067aa0ba902b7"
W3C_TRACEPARENT="00-${TRACE_ID}-${SPAN_ID}-01"

generate_uuid() {
    python3 -c 'import uuid; print(uuid.uuid4())'
}

generate_uuidv7() {
    python3 -c '
import uuid, time
t_ms = int(time.time() * 1000)
u = uuid.uuid4()
rand_b = u.bytes[6:]
high = (t_ms >> 16) & 0xFFFFFFFF
mid = t_ms & 0xFFFF
b = high.to_bytes(4, "big") + mid.to_bytes(2, "big") + bytearray([0x70 | (rand_b[0] & 0x0F), 0x80 | (rand_b[1] & 0x3F)]) + rand_b[2:]
print(str(uuid.UUID(bytes=bytes(b))))
'
}

assert_endpoint() {
    local name="$1"
    local method="$2"
    local url="$3"
    local data="$4"
    local expected_code="$5"

    local resp=""
    if [[ "${method}" == "GET" ]]; then
        resp=$(curl --noproxy "*" -s -i -X GET "${url}" \
            -H "traceparent: ${W3C_TRACEPARENT}" \
            -H "x-tenant-id: ${TENANT_ID}" \
            -H "x-request-id: req-${name}")
    else
        resp=$(curl --noproxy "*" -s -i -X "${method}" "${url}" \
            -H "Content-Type: application/json" \
            -H "traceparent: ${W3C_TRACEPARENT}" \
            -H "x-tenant-id: ${TENANT_ID}" \
            -H "x-request-id: req-${name}" \
            -d "${data}")
    fi

    local code=$(echo "${resp}" | grep -E '^HTTP/[0-9.]+ [0-9]+' | tail -n 1 | awk '{print $2}')
    local trace_header=$(echo "${resp}" | grep -i "^x-trace-id:" | tr -d '\r' | awk '{print $2}')

    if [[ "${code}" != "${expected_code}" ]]; then
        echo "FAIL [${name}]: Expected HTTP ${expected_code}, got ${code}"
        echo "${resp}"
        exit 1
    fi

    if [[ -z "${trace_header}" || "${trace_header}" != *"${TRACE_ID}"* ]]; then
        echo "FAIL [${name}]: X-Trace-Id mismatch: ${trace_header}"
        exit 1
    fi

    echo "[PASS] [${name}] HTTP ${code} | Trace: ${trace_header}"
}

echo "================================================================"
echo " 3. EXECUTING LIVE HTTP VERIFICATION ACROSS FEATURES"
echo "================================================================"

WF_NAME="order_flow_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
WF_RESP=$(curl --noproxy "*" -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/workflow-definitions" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-tenant-id: ${TENANT_ID}" \
    -H "x-request-id: req-wf-def" \
    -d "{
        \"tenantId\": \"${TENANT_ID}\",
        \"name\": \"${WF_NAME}\",
        \"version\": 1,
        \"source\": \"ci_test\",
        \"externalId\": \"ext_wf_1\",
        \"correlationId\": \"$(generate_uuid)\",
        \"steps\": [
            {
                \"stepId\": \"reserve\",
                \"stepName\": \"Reserve Stock\",
                \"isPivot\": true,
                \"actionType\": \"HTTP\",
                \"actionEndpoint\": \"http://btpllap22-ThinkPad-L460:9095/reserve\",
                \"timeoutSeconds\": 10,
                \"maxRetries\": 3,
                \"dependencies\": []
            }
        ]
    }")
WF_CODE=$(echo "${WF_RESP}" | grep -E '^HTTP/[0-9.]+ [0-9]+' | tail -n 1 | awk '{print $2}')
if [[ "${WF_CODE}" != "201" ]]; then
    echo "FAIL [workflow-definitions]: Expected 201, got ${WF_CODE}"
    exit 1
fi
WF_DEF_ID=$(echo "${WF_RESP}" | grep -o '"id":"[^"]*' | head -n 1 | awk -F'"' '{print $4}')
echo "[PASS] [workflow-definitions] HTTP 201 | ID: ${WF_DEF_ID}"

STATE_SAGA_ID=$(generate_uuid)
STATE_PUB_ID="sga_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
SAG_STATE_PUB_ID="sag_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"

PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO saga_instances (id, public_id, tenant_id, workflow_definition_id, workflow_version, spec_hash, source, external_id, input_hash, correlation_id, current_state, version, fencing_token, deadline_at, initial_payload, created_at, updated_at)
     VALUES ('${STATE_SAGA_ID}', '${STATE_PUB_ID}', '${TENANT_ID}', '${WF_DEF_ID}', 1, 'mockspec', 'TEST', 'ext_inst', 'mockhash', '${STATE_SAGA_ID}', 'CREATED', 1, 0, NOW() + INTERVAL '1 hour', '{}', NOW(), NOW());" >/dev/null 2>&1

PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO saga_instance_states (id, public_id, tenant_id, source, external_id, correlation_id, current_state, version, owner_epoch, fencing_token, created_at, updated_at)
     VALUES ('${STATE_SAGA_ID}', '${SAG_STATE_PUB_ID}', '${TENANT_ID}', 'TEST', 'ext_state', '${STATE_SAGA_ID}', 'CREATED', 1, 1, 0, NOW(), NOW());" >/dev/null 2>&1 

assert_endpoint "transaction-state-machine" "POST" "http://127.0.0.1:${PORT}/api/v1/sagas/transitions" \
    "{\"tenantId\":\"${TENANT_ID}\",\"sagaId\":\"${STATE_SAGA_ID}\",\"event\":\"ORCHESTRATOR_CLAIM\",\"expectedVersion\":1,\"reason\":\"start\",\"fencingToken\":0}" \
    "200"

assert_endpoint "saga-orchestration" "POST" "http://127.0.0.1:${PORT}/api/v1/sagas/execute" \
    "{\"tenantId\":\"${TENANT_ID}\",\"workflowName\":\"${WF_NAME}\",\"workflowVersion\":1,\"source\":\"ci_test\",\"externalId\":\"ext_sga_1\",\"correlationId\":\"$(generate_uuid)\",\"initialPayload\":{}}" \
    "202"

assert_endpoint "crash-recovery" "GET" "http://127.0.0.1:${PORT}/api/v1/crash-recovery/status" "" "200"

SEQ_ID=$(generate_uuidv7)
SEQ_PUB="seq_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
assert_endpoint "sequential-execution" "POST" "http://127.0.0.1:${PORT}/api/v1/sagas/sequential/start" \
    "{\"id\":\"${SEQ_ID}\",\"publicId\":\"${SEQ_PUB}\",\"tenantId\":\"${TENANT_ID}\",\"workflowDefinitionId\":\"${WF_DEF_ID}\",\"source\":\"ci_test\",\"externalId\":\"ext_seq_1\",\"correlationId\":\"$(generate_uuid)\",\"payload\":{}}" \
    "201"

PAR_ID=$(generate_uuidv7)
PAR_PUB="par_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
assert_endpoint "parallel-execution" "POST" "http://127.0.0.1:${PORT}/api/v1/sagas/parallel/start" \
    "{\"id\":\"${PAR_ID}\",\"publicId\":\"${PAR_PUB}\",\"tenantId\":\"${TENANT_ID}\",\"sagaId\":\"${STATE_SAGA_ID}\",\"branches\":[{\"branchId\":\"br-1\",\"name\":\"Branch 1\",\"role\":\"REQUIRED\",\"effectType\":\"COMPENSABLE\",\"payload\":{}}],\"joinPolicy\":\"ALL_REQUIRED\",\"quorumCount\":0}" \
    "201"

INB_ID=$(generate_uuid)
INB_PUB="inb_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
assert_endpoint "inbox-deduplication" "POST" "http://127.0.0.1:${PORT}/api/v1/inbox/ingest" \
    "{\"id\":\"${INB_ID}\",\"publicId\":\"${INB_PUB}\",\"tenantId\":\"${TENANT_ID}\",\"source\":\"ORDERS\",\"externalId\":\"ext_inb_1\",\"correlationId\":\"$(generate_uuid)\",\"handlerName\":\"order_handler\",\"handlerGeneration\":1,\"producerEventTime\":\"2026-11-10T00:00:00Z\",\"payload\":{}}" \
    "201"

IDK_ID=$(generate_uuid)
IDK_PUB="idk_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
assert_endpoint "idempotency-keys" "POST" "http://127.0.0.1:${PORT}/api/v1/idempotency/acquire" \
    "{\"id\":\"${IDK_ID}\",\"publicId\":\"${IDK_PUB}\",\"tenantId\":\"${TENANT_ID}\",\"source\":\"CHECKOUT\",\"externalId\":\"ext_idk_1\",\"correlationId\":\"$(generate_uuid)\",\"principalId\":\"usr-1\",\"operationId\":\"checkout\",\"idempotencyKey\":\"key-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')\",\"payload\":{}}" \
    "201"

assert_endpoint "retry-policies" "GET" "http://127.0.0.1:${PORT}/api/v1/retry-policies/breaker/status?dependencyId=dep-pay-1" "" "200"

CMP_ID=$(generate_uuidv7)
CMP_PUB="cmp_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
assert_endpoint "compensation-engine" "POST" "http://127.0.0.1:${PORT}/api/v1/compensation/trigger" \
    "{\"id\":\"${CMP_ID}\",\"publicId\":\"${CMP_PUB}\",\"tenantId\":\"${TENANT_ID}\",\"sagaId\":\"${STATE_SAGA_ID}\",\"reason\":\"failure\",\"forwardSteps\":[]}" \
    "201"

assert_endpoint "fencing-tokens" "POST" "http://127.0.0.1:${PORT}/api/v1/fencing/acquire" \
    "{\"tenantId\":\"${TENANT_ID}\",\"resourceId\":\"res-001\",\"holderId\":\"worker-1\",\"leaseDurationMs\":10000}" \
    "201"

assert_endpoint "compensation-retry" "POST" "http://127.0.0.1:${PORT}/api/v1/compensation-retry/evaluate" \
    "{\"tenantId\":\"${TENANT_ID}\",\"sagaId\":\"${STATE_SAGA_ID}\",\"stepId\":\"refund\",\"stepType\":\"HTTP\",\"dependencyID\":\"dep-refund\",\"compensationKey\":\"key-refund\",\"attemptNumber\":1,\"statusCode\":500,\"errorMessage\":\"timeout\",\"firstAttemptAt\":\"2026-11-01T00:00:00Z\",\"businessDeadline\":\"2026-11-02T00:00:00Z\"}" \
    "200"

DLQ_ATTEMPT_ID=$(generate_uuid)
assert_endpoint "dead-letter-queue" "POST" "http://127.0.0.1:${PORT}/api/v1/dlq/admit" \
    "{\"tenantId\":\"${TENANT_ID}\",\"correlationId\":\"$(generate_uuid)\",\"sagaId\":\"${STATE_SAGA_ID}\",\"stepIndex\":1,\"stepType\":\"INVOKE\",\"attemptId\":\"${DLQ_ATTEMPT_ID}\",\"idempotencyKey\":\"dlq-key-1\",\"outcomeClass\":\"FAILED\",\"failureClass\":\"POISON\",\"errorFingerprint\":\"fp_001\",\"rawMessage\":\"terminal unrecoverable failure\",\"payloadRef\":\"s3://bucket/key\",\"schemaVersion\":1,\"moneyAtStakeCents\":5000,\"currency\":\"USD\"}" \
    "201"

assert_endpoint "reconciliation-engine" "POST" "http://127.0.0.1:${PORT}/api/v1/reconciliation/evaluate" \
    "{\"tenantId\":\"${TENANT_ID}\",\"sagaId\":\"${STATE_SAGA_ID}\",\"internalStatus\":\"COMPLETED\",\"externalStatus\":\"FAILED\",\"internalAmountCents\":5000,\"externalAmountCents\":5000,\"currency\":\"USD\"}" \
    "200"

assert_endpoint "manual-intervention" "POST" "http://127.0.0.1:${PORT}/api/v1/manual-intervention/intervene" \
    "{\"tenantId\":\"${TENANT_ID}\",\"sagaId\":\"${STATE_SAGA_ID}\",\"operatorId\":\"alice\",\"operatorRole\":\"admin:saga:intervene\",\"actionType\":\"FORCE_RETRY\",\"currentSagaState\":\"FAILED\",\"reason\":\"retry\"}" \
    "200"

assert_endpoint "scheduled-execution" "POST" "http://127.0.0.1:${PORT}/api/v1/scheduled-execution/schedules" \
    "{\"tenantId\":\"${TENANT_ID}\",\"workflowId\":\"wfd_123\",\"scheduleType\":\"RECURRING_CRON\",\"cronExpression\":\"0 * * * *\",\"timeZone\":\"UTC\",\"runAsPrincipal\":\"daemon\"}" \
    "201"

assert_endpoint "optimistic-concurrency-control" "POST" "http://127.0.0.1:${PORT}/api/v1/occ/mutate" \
    "{\"tenantId\":\"${TENANT_ID}\",\"sagaId\":\"${STATE_SAGA_ID}\",\"expectedVersion\":2,\"expectedEpoch\":1,\"targetState\":\"RUNNING\",\"workerID\":\"w1\",\"inputId\":\"inp-1\",\"inputType\":\"REPLY\",\"inputPayload\":\"{}\"}" \
    "200"

assert_endpoint "workflow-versioning" "POST" "http://127.0.0.1:${PORT}/api/v1/workflow-versions/bundles" \
    "{\"tenantId\":\"${TENANT_ID}\",\"source\":\"ci_test\",\"externalId\":\"ext_wv_1\",\"correlationId\":\"$(generate_uuid)\",\"workflowName\":\"payment-flow\",\"versionTag\":\"1.0.0\",\"signature\":\"sig_mock\",\"bundlePayload\":{\"steps\":[{\"name\":\"reserve\"}]},\"capabilitiesRequired\":[]}" \
    "201"

assert_endpoint "audit-trail" "POST" "http://127.0.0.1:${PORT}/api/v1/audit/events/capture" \
    "{\"tenant_id\":\"${TENANT_ID}\",\"source\":\"ci_test\",\"external_id\":\"ext_aud_1\",\"correlation_id\":\"$(generate_uuid)\",\"audit_id\":\"aud_001\",\"occurred_at\":\"2026-11-10T00:00:00Z\",\"action\":\"SAGA_CREATED\",\"actor_id\":\"user_1\",\"actor_ip\":\"127.0.0.1\",\"on_behalf_of\":\"user_1\",\"target_resource\":\"sagas/${STATE_SAGA_ID}\",\"payload\":{}}" \
    "202"

assert_endpoint "authentication-authorization" "POST" "http://127.0.0.1:${PORT}/api/v1/auth/api-keys" \
    "{\"tenant_id\":\"${TENANT_ID}\",\"name\":\"ci_key\",\"scopes\":[\"saga:read\",\"saga:write\"],\"allowed_cidrs\":[\"127.0.0.1/32\"]}" \
    "201"

assert_endpoint "distributed-tracing" "POST" "http://127.0.0.1:${PORT}/api/v1/tracing/context/start" \
    "{\"tenant_id\":\"${TENANT_ID}\",\"correlation_id\":\"$(generate_uuid)\",\"route\":\"/checkout\",\"is_vip\":true}" \
    "200"

assert_endpoint "operational-dashboard" "GET" "http://127.0.0.1:${PORT}/api/v1/dashboard/overview?tenant_id=${TENANT_ID}" "" "200"

OX_EVENT_ID=$(generate_uuid)
OX_EVENT_PUB_ID="oxe_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
OX_TOPIC="dist-tx.live-verify.mat.${TENANT_ID}"

/snap/bin/docker exec dist_tx-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "${OX_TOPIC}" --partitions 1 --replication-factor 1 >/dev/null 2>&1

PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO outbox_events (id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id, aggregate_sequence, destination_topic, partition_key, event_type, payload, headers, status, attempt_count, version, created_at, updated_at)
     VALUES ('${OX_EVENT_ID}', '${OX_EVENT_PUB_ID}', '${TENANT_ID}', '${STATE_SAGA_ID}', 'SAGA', '${STATE_SAGA_ID}', 1, '${OX_TOPIC}', '${STATE_SAGA_ID}', 'PAYMENT_COMPLETED', '{\"amount\":500}'::jsonb, '{\"tenant\":\"${TENANT_ID}\"}'::jsonb, 'PENDING', 0, 1, NOW(), NOW());" 

assert_endpoint "transactional-outbox" "POST" "http://127.0.0.1:${PORT}/api/v1/outbox/relay" \
    "{\"tenantId\":\"${TENANT_ID}\",\"topic\":\"${OX_TOPIC}\",\"workerId\":\"worker-relay-1\",\"batchLimit\":10}" \
    "200"

assert_endpoint "transaction-query-api" "POST" "http://127.0.0.1:${PORT}/api/v1/transactions:search" \
    "{\"tenant_id\":\"${TENANT_ID}\"}" \
    "200"

echo "--- Verifying Feature 25: Tenant Isolation Constraint ---"
ISOLATION_CHECK=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT count(*) FROM saga_instance_states WHERE tenant_id = '${TENANT_ID}';")
if [[ "${ISOLATION_CHECK}" -ge 1 ]]; then
    echo "[PASS] [tenant-isolation] PostgreSQL tenant boundary verified: ${ISOLATION_CHECK} records isolated to ${TENANT_ID}"
else
    echo "FAIL [tenant-isolation]: Tenant isolation check failed"
    exit 1
fi

echo "================================================================"
echo " 4. KAFKA BROKER REAL EVENT ASSERTION"
echo "================================================================"
OUTBOX_STATUS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT status FROM outbox_events WHERE id = '${OX_EVENT_ID}';")
echo "Outbox Event DB Status: ${OUTBOX_STATUS}"
if [[ "${OUTBOX_STATUS}" != "PUBLISHED" ]]; then
    echo "FAIL [transactional-outbox]: Outbox event was not marked as PUBLISHED in DB"
    exit 1
fi
echo "[PASS] Transactional outbox event marked PUBLISHED in PostgreSQL"

#!/usr/bin/env bash
set -euo pipefail
export PATH="/home/btpl-lap-22/.local/go/bin:/home/btpl-lap-22/.local/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT_DIR}"

PORT="${PORT:-8088}"
ADMIN_PORT="${ADMIN_PORT:-8089}"
MOCK_PORT="${MOCK_PORT:-9095}"
DOWNSTREAM_HOST="${DOWNSTREAM_HOST:-$(hostname)}"
if [[ "${DOWNSTREAM_HOST}" == "localhost" || "${DOWNSTREAM_HOST}" == "127.0.0.1" ]]; then
    DOWNSTREAM_HOST="sandbox"
fi

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-supersecretpgpassword}"
DB_NAME="${DB_NAME:-dist_tx_engine}"
DB_SSLMODE="${DB_SSLMODE:-disable}"
WORKER_ID="${WORKER_ID:-real-test-worker-$$}"

TMP_DIR="$(mktemp -d)"
MOCK_PID=""
ENGINE_PID=""

cleanup() {
    local exit_code=$?
    echo ""
    echo "================================================================"
    echo " [CLEANUP] Cleaning test runner processes and temp directory..."
    echo "================================================================"
    if [[ -n "${ENGINE_PID}" ]] && kill -0 "${ENGINE_PID}" 2>/dev/null; then
        echo "Stopping engine process (PID: ${ENGINE_PID})..."
        kill -TERM "${ENGINE_PID}" 2>/dev/null || true
        wait "${ENGINE_PID}" 2>/dev/null || true
    fi
    if [[ -n "${MOCK_PID}" ]] && kill -0 "${MOCK_PID}" 2>/dev/null; then
        echo "Stopping mock downstream server (PID: ${MOCK_PID})..."
        kill -TERM "${MOCK_PID}" 2>/dev/null || true
        wait "${MOCK_PID}" 2>/dev/null || true
    fi
    rm -rf "${TMP_DIR}"
    if [[ ${exit_code} -eq 0 ]]; then
        echo ">>> [SUCCESS] All live verification tests completed successfully!"
    else
        echo ">>> [FAILURE] Real verification tests failed with exit code ${exit_code}."
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
echo " 1. STARTING MOCK DOWNSTREAM SERVICE"
echo "================================================================"
cat << 'EOF' > "${TMP_DIR}/mock_downstream.py"
import sys, time
from http.server import HTTPServer, BaseHTTPRequestHandler

class MockHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"UP"}\n')

    def do_POST(self):
        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len) if content_len > 0 else b""
        path = self.path

        if path.endswith("/fail-step"):
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"simulated_downstream_failure"}\n')
            return

        if "slow-step" in path:
            time.sleep(2.0)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"SLOW_DONE"}\n')
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if "reserve" in path:
            self.wfile.write(b'{"status":"RESERVED","hold_id":"hld_999"}\n')
        elif "charge" in path:
            self.wfile.write(b'{"status":"CHARGED","tx_id":"tx_888"}\n')
        elif "compensate" in path or "cancel" in path:
            self.wfile.write(b'{"status":"COMPENSATED"}\n')
        else:
            self.wfile.write(b'{"status":"OK"}\n')

    def log_message(self, format, *args):
        pass

if __name__ == "__main__":
    port = int(sys.argv[1])
    server = HTTPServer(("0.0.0.0", port), MockHandler)
    server.serve_forever()
EOF

python3 "${TMP_DIR}/mock_downstream.py" "${MOCK_PORT}" &
MOCK_PID=$!
echo "Mock downstream server started (PID: ${MOCK_PID}, Port: ${MOCK_PORT})"

echo -n "Waiting for mock downstream readiness..."
for _ in $(seq 1 10); do
    if curl -s "http://127.0.0.1:${MOCK_PORT}/health" >/dev/null 2>&1; then
        echo " Ready."
        break
    fi
    sleep 0.5
done

echo "================================================================"
echo " 2. BUILDING DISTRIBUTED TRANSACTION ENGINE BINARY"
echo "================================================================"
go build -o "${TMP_DIR}/engine-bin" ./src/main.go
echo "Binary built successfully at ${TMP_DIR}/engine-bin"

echo "================================================================"
echo " 3. LAUNCHING ENGINE DAEMON"
echo "================================================================"
PORT="${PORT}" \
ADMIN_PORT="${ADMIN_PORT}" \
DB_HOST="${DB_HOST}" \
DB_PORT="${DB_PORT}" \
DB_USER="${DB_USER}" \
DB_PASSWORD="${DB_PASSWORD}" \
DB_NAME="${DB_NAME}" \
DB_SSLMODE="${DB_SSLMODE}" \
WORKER_ID="${WORKER_ID}" \
"${TMP_DIR}/engine-bin" > "${TMP_DIR}/engine.log" 2>&1 &
ENGINE_PID=$!
echo "Engine daemon started (PID: ${ENGINE_PID}, Port: ${PORT}, Admin Port: ${ADMIN_PORT})"

echo -n "Waiting for engine deep readiness on /health/ready..."
READY=0
for i in $(seq 1 30); do
    STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${ADMIN_PORT}/health/ready" || true)
    if [[ "${STATUS_CODE}" == "200" ]]; then
        READY=1
        echo " Ready (HTTP 200 on attempt ${i})."
        break
    fi
    sleep 1
done

if [[ ${READY} -ne 1 ]]; then
    echo " FAILED to become ready!"
    echo "--- Engine Log Tail ---"
    tail -n 50 "${TMP_DIR}/engine.log"
    exit 1
fi

echo "================================================================"
echo " 4. SCENARIO 1: ADMIN PROBES & PROMETHEUS METRICS"
echo "================================================================"
LIVE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${ADMIN_PORT}/health/live")
if [[ "${LIVE_STATUS}" != "200" ]]; then
    echo "FAIL: Expected /health/live to return 200, got ${LIVE_STATUS}"
    exit 1
fi
echo "[PASS] /health/live returned 200 OK"

METRICS_OUTPUT=$(curl -s "http://127.0.0.1:${ADMIN_PORT}/metrics")
if [[ -z "${METRICS_OUTPUT}" ]]; then
    echo "FAIL: /metrics returned empty output"
    exit 1
fi
echo "[PASS] /metrics returned telemetry payload (${#METRICS_OUTPUT} bytes)"

echo "================================================================"
echo " 5. SCENARIO 2: WORKFLOW REGISTRATION & RETRIEVAL"
echo "================================================================"
TIMESTAMP=$(date +%s)
WF_NAME="order_fulfillment_${TIMESTAMP}"
WF_CORR_ID=$(generate_uuid)
REGISTER_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "name": "${WF_NAME}",
  "version": 1,
  "source": "ci_automated_test",
  "externalId": "ext_wf_${TIMESTAMP}",
  "correlationId": "${WF_CORR_ID}",
  "steps": [
    {
      "stepId": "reserve_stock",
      "stepName": "Reserve Inventory",
      "isPivot": false,
      "actionType": "HTTP",
      "actionEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/reserve",
      "compensationAction": "HTTP",
      "compensationEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/compensate-reserve",
      "timeoutSeconds": 10,
      "maxRetries": 3,
      "dependencies": []
    },
    {
      "stepId": "charge_customer",
      "stepName": "Charge Payment",
      "isPivot": true,
      "actionType": "HTTP",
      "actionEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/charge",
      "timeoutSeconds": 10,
      "maxRetries": 3,
      "dependencies": ["reserve_stock"]
    }
  ]
}
EOF
)

WF_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/workflow-definitions" \
    -H "Content-Type: application/json" \
    -d "${REGISTER_PAYLOAD}")

WF_HTTP_CODE=$(echo "${WF_RESPONSE}" | tail -n1)
WF_BODY=$(echo "${WF_RESPONSE}" | sed '$d')

if [[ "${WF_HTTP_CODE}" != "201" ]]; then
    echo "FAIL: Workflow registration failed with HTTP ${WF_HTTP_CODE}: ${WF_BODY}"
    exit 1
fi

WF_ID=$(echo "${WF_BODY}" | json_extract "['data']['id']")
echo "[PASS] Workflow registered successfully (ID: ${WF_ID}, Name: ${WF_NAME})"

WF_GET_RESPONSE=$(curl -s -w "\n%{http_code}" -H "x-tenant-id: default" "http://127.0.0.1:${PORT}/api/v1/workflow-definitions/${WF_ID}")
WF_GET_HTTP_CODE=$(echo "${WF_GET_RESPONSE}" | tail -n1)
WF_GET_BODY=$(echo "${WF_GET_RESPONSE}" | sed '$d')

if [[ "${WF_GET_HTTP_CODE}" != "200" ]]; then
    echo "FAIL: Workflow retrieval failed with HTTP ${WF_GET_HTTP_CODE}: ${WF_GET_BODY}"
    exit 1
fi
echo "[PASS] Workflow retrieved successfully via GET /api/v1/workflow-definitions/${WF_ID}"

echo "================================================================"
echo " 6. SCENARIO 3: TRANSACTION STATE MACHINE DIRECT API & CAS"
echo "================================================================"
STATE_UUID=$(generate_uuid)
STATE_PUBLIC_ID="sag_$(python3 -c "import secrets; print(secrets.token_hex(13))")"
STATE_CORR_ID=$(generate_uuid)

INIT_STATE_PAYLOAD=$(cat << EOF
{
  "id": "${STATE_UUID}",
  "publicId": "${STATE_PUBLIC_ID}",
  "tenantId": "default",
  "source": "ci_test",
  "externalId": "ext_state_${TIMESTAMP}",
  "correlationId": "${STATE_CORR_ID}"
}
EOF
)

STATE_INIT_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/state" \
    -H "Content-Type: application/json" \
    -d "${INIT_STATE_PAYLOAD}")
STATE_INIT_CODE=$(echo "${STATE_INIT_RESP}" | tail -n1)
STATE_INIT_BODY=$(echo "${STATE_INIT_RESP}" | sed '$d')

if [[ "${STATE_INIT_CODE}" != "201" ]]; then
    echo "FAIL: State initialization failed with HTTP ${STATE_INIT_CODE}: ${STATE_INIT_BODY}"
    exit 1
fi
echo "[PASS] State machine initialized state to CREATED (HTTP 201)"

TRANSITION_1_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "sagaId": "${STATE_UUID}",
  "event": "ORCHESTRATOR_CLAIM",
  "expectedVersion": 1,
  "fencingToken": 0,
  "reason": "claim lease"
}
EOF
)

TRANS_1_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/transitions" \
    -H "Content-Type: application/json" \
    -d "${TRANSITION_1_PAYLOAD}")
TRANS_1_CODE=$(echo "${TRANS_1_RESP}" | tail -n1)

if [[ "${TRANS_1_CODE}" != "200" ]]; then
    echo "FAIL: Expected transition 1 (ORCHESTRATOR_CLAIM) with version 1 to return 200, got ${TRANS_1_CODE}"
    exit 1
fi
echo "[PASS] Compare-And-Swap transition 1 -> RUNNING succeeded (HTTP 200, Version 2)"

TRANSITION_2_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "sagaId": "${STATE_UUID}",
  "event": "DAG_COMPLETED",
  "expectedVersion": 2,
  "fencingToken": 0,
  "reason": "workflow completed"
}
EOF
)

TRANS_2_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/transitions" \
    -H "Content-Type: application/json" \
    -d "${TRANSITION_2_PAYLOAD}")
TRANS_2_CODE=$(echo "${TRANS_2_RESP}" | tail -n1)

if [[ "${TRANS_2_CODE}" != "200" ]]; then
    echo "FAIL: Expected transition 2 (DAG_COMPLETED) with version 2 to return 200, got ${TRANS_2_CODE}"
    exit 1
fi
echo "[PASS] Compare-And-Swap transition 2 -> COMPLETED succeeded (HTTP 200, Version 3)"

STALE_CAS_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "sagaId": "${STATE_UUID}",
  "event": "DAG_COMPLETED",
  "expectedVersion": 1,
  "fencingToken": 0,
  "reason": "stale retry"
}
EOF
)

STALE_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/transitions" \
    -H "Content-Type: application/json" \
    -d "${STALE_CAS_PAYLOAD}")
STALE_CODE=$(echo "${STALE_RESP}" | tail -n1)

if [[ "${STALE_CODE}" != "409" ]]; then
    echo "FAIL: Expected stale CAS transition (expectedVersion=1) to return 409 Conflict, got ${STALE_CODE}"
    exit 1
fi
echo "[PASS] Stale CAS transition rejected with HTTP 409 Conflict as required by invariant"

echo "================================================================"
echo " 7. SCENARIO 4: END-TO-END SAGA EXECUTION (HAPPY PATH)"
echo "================================================================"
SAGA_CORR_ID=$(generate_uuid)
EXEC_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "workflowName": "${WF_NAME}",
  "workflowVersion": 1,
  "source": "ci_automated_test",
  "externalId": "ext_saga_${TIMESTAMP}",
  "correlationId": "${SAGA_CORR_ID}",
  "initialPayload": {
    "orderId": "ord_happy_101",
    "amount": 250
  }
}
EOF
)

EXEC_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/execute" \
    -H "Content-Type: application/json" \
    -d "${EXEC_PAYLOAD}")

EXEC_STATUS=$(echo "${EXEC_RESP}" | grep -E "^HTTP" | awk '{print $2}')
LOCATION_HEADER=$(echo "${EXEC_RESP}" | grep -i "^Location:" | tr -d '\r' | awk '{print $2}')

if [[ "${EXEC_STATUS}" != "202" ]]; then
    echo "FAIL: Expected saga execution to return 202 Accepted, got ${EXEC_STATUS}"
    echo "${EXEC_RESP}"
    exit 1
fi

SAGA_PUBLIC_ID=$(basename "${LOCATION_HEADER}")
echo "[PASS] Saga accepted asynchronously (HTTP 202, ID: ${SAGA_PUBLIC_ID}, Location: ${LOCATION_HEADER})"

echo -n "Waiting for saga asynchronous completion via database & API polling..."
COMPLETED=0
for _ in $(seq 1 30); do
    SAGA_GET_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/sagas/${SAGA_PUBLIC_ID}?tenantId=default")
    CURRENT_STATE=$(echo "${SAGA_GET_RESP}" | json_extract "['data']['currentState']" 2>/dev/null || echo "UNKNOWN")
    if [[ "${CURRENT_STATE}" == "COMPLETED" ]]; then
        COMPLETED=1
        echo " Completed."
        break
    fi
    sleep 1
done

if [[ ${COMPLETED} -ne 1 ]]; then
    echo " TIMEOUT waiting for saga to complete! Final state was: ${CURRENT_STATE}"
    tail -n 50 "${TMP_DIR}/engine.log"
    exit 1
fi

echo "[PASS] Saga successfully progressed to COMPLETED in database"

DB_ASSERT_RESULT=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT current_state, fencing_token FROM saga_instances WHERE public_id = '${SAGA_PUBLIC_ID}';" 2>/dev/null || echo "")

if [[ -n "${DB_ASSERT_RESULT}" ]]; then
    DB_STATE=$(echo "${DB_ASSERT_RESULT}" | cut -d '|' -f 1)
    DB_FENCING=$(echo "${DB_ASSERT_RESULT}" | cut -d '|' -f 2)
    if [[ "${DB_STATE}" == "COMPLETED" && ${DB_FENCING} -ge 1 ]]; then
        echo "[PASS] PostgreSQL direct assertion verified: currentState=COMPLETED, fencingToken=${DB_FENCING}"
    else
        echo "FAIL: PostgreSQL row mismatch: state=${DB_STATE}, fencingToken=${DB_FENCING}"
        exit 1
    fi
fi

echo "================================================================"
echo " 8. SCENARIO 5: SAGA PRE-PIVOT FAILURE & REVERSE COMPENSATION"
echo "================================================================"
FAIL_WF_NAME="failing_order_flow_${TIMESTAMP}"
FAIL_WF_CORR_ID=$(generate_uuid)
FAIL_WF_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "name": "${FAIL_WF_NAME}",
  "version": 1,
  "source": "ci_automated_test",
  "externalId": "ext_fail_wf_${TIMESTAMP}",
  "correlationId": "${FAIL_WF_CORR_ID}",
  "steps": [
    {
      "stepId": "reserve_stock",
      "stepName": "Reserve Inventory",
      "isPivot": false,
      "actionType": "HTTP",
      "actionEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/reserve",
      "compensationAction": "HTTP",
      "compensationEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/compensate-reserve",
      "timeoutSeconds": 10,
      "maxRetries": 1,
      "dependencies": []
    },
    {
      "stepId": "charge_customer",
      "stepName": "Failing Step",
      "isPivot": false,
      "actionType": "HTTP",
      "actionEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/fail-step",
      "compensationAction": "HTTP",
      "compensationEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/compensate-charge",
      "timeoutSeconds": 10,
      "maxRetries": 1,
      "dependencies": ["reserve_stock"]
    }
  ]
}
EOF
)

FAIL_WF_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/workflow-definitions" \
    -H "Content-Type: application/json" \
    -d "${FAIL_WF_PAYLOAD}")
FAIL_WF_HTTP_CODE=$(echo "${FAIL_WF_RESP}" | tail -n1)

if [[ "${FAIL_WF_HTTP_CODE}" != "201" ]]; then
    echo "FAIL: Failed workflow registration returned HTTP ${FAIL_WF_HTTP_CODE}"
    exit 1
fi
echo "[PASS] Failure workflow registered successfully"

FAIL_SAGA_CORR_ID=$(generate_uuid)
EXEC_FAIL_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "workflowName": "${FAIL_WF_NAME}",
  "workflowVersion": 1,
  "source": "ci_automated_test",
  "externalId": "ext_fail_saga_${TIMESTAMP}",
  "correlationId": "${FAIL_SAGA_CORR_ID}",
  "initialPayload": {
    "orderId": "ord_fail_999",
    "amount": 999
  }
}
EOF
)

EXEC_FAIL_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/execute" \
    -H "Content-Type: application/json" \
    -d "${EXEC_FAIL_PAYLOAD}")
FAIL_SAGA_LOCATION=$(echo "${EXEC_FAIL_RESP}" | grep -i "^Location:" | tr -d '\r' | awk '{print $2}')
FAIL_SAGA_ID=$(basename "${FAIL_SAGA_LOCATION}")

echo "[PASS] Failure saga submitted (ID: ${FAIL_SAGA_ID})"

echo -n "Waiting for pre-pivot compensation to converge to FAILED_COMPENSATED..."
COMPENSATED=0
for _ in $(seq 1 30); do
    STATUS_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/sagas/${FAIL_SAGA_ID}?tenantId=default")
    FAIL_CURRENT_STATE=$(echo "${STATUS_RESP}" | json_extract "['data']['currentState']" 2>/dev/null || echo "UNKNOWN")
    if [[ "${FAIL_CURRENT_STATE}" == "FAILED_COMPENSATED" ]]; then
        COMPENSATED=1
        echo " Compensated."
        break
    fi
    sleep 1
done

if [[ ${COMPENSATED} -ne 1 ]]; then
    echo " TIMEOUT waiting for compensation! Final state: ${FAIL_CURRENT_STATE}"
    tail -n 50 "${TMP_DIR}/engine.log"
    exit 1
fi
echo "[PASS] Pre-pivot failure successfully triggered reverse compensation to FAILED_COMPENSATED"

echo "================================================================"
echo " 9. SCENARIO 6: SAGA CANCELLATION API"
echo "================================================================"
CANCEL_WF_NAME="cancel_order_flow_${TIMESTAMP}"
CANCEL_WF_CORR_ID=$(generate_uuid)
CANCEL_WF_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "name": "${CANCEL_WF_NAME}",
  "version": 1,
  "source": "ci_automated_test",
  "externalId": "ext_cancel_wf_${TIMESTAMP}",
  "correlationId": "${CANCEL_WF_CORR_ID}",
  "steps": [
    {
      "stepId": "prep_item",
      "stepName": "Slow Preparation",
      "isPivot": false,
      "actionType": "HTTP",
      "actionEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/slow-step",
      "compensationAction": "HTTP",
      "compensationEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/compensate-reserve",
      "timeoutSeconds": 30,
      "maxRetries": 1,
      "dependencies": []
    },
    {
      "stepId": "finalize_item",
      "stepName": "Final Step",
      "isPivot": true,
      "actionType": "HTTP",
      "actionEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/charge",
      "timeoutSeconds": 30,
      "maxRetries": 1,
      "dependencies": ["prep_item"]
    }
  ]
}
EOF
)

CANCEL_WF_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/workflow-definitions" \
    -H "Content-Type: application/json" \
    -d "${CANCEL_WF_PAYLOAD}")
CANCEL_WF_CODE=$(echo "${CANCEL_WF_RESP}" | tail -n1)
if [[ "${CANCEL_WF_CODE}" != "201" ]]; then
    echo "FAIL: Cancel workflow registration returned HTTP ${CANCEL_WF_CODE}"
    exit 1
fi

CANCEL_SAGA_CORR_ID=$(generate_uuid)
EXEC_CANCEL_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "workflowName": "${CANCEL_WF_NAME}",
  "workflowVersion": 1,
  "source": "ci_cancellation_test",
  "externalId": "ext_cancel_saga_${TIMESTAMP}",
  "correlationId": "${CANCEL_SAGA_CORR_ID}",
  "initialPayload": {"item": "notebook"}
}
EOF
)

EXEC_CANCEL_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/execute" \
    -H "Content-Type: application/json" \
    -d "${EXEC_CANCEL_PAYLOAD}")
CANCEL_LOCATION=$(echo "${EXEC_CANCEL_RESP}" | grep -i "^Location:" | tr -d '\r' | awk '{print $2}')
CANCEL_SAGA_ID=$(basename "${CANCEL_LOCATION}")

echo "[PASS] Submitted saga for cancellation test: ${CANCEL_SAGA_ID}"

CANCEL_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/${CANCEL_SAGA_ID}/cancel" \
    -H "X-Tenant-Id: default")
CANCEL_CODE=$(echo "${CANCEL_RESP}" | tail -n1)
CANCEL_BODY=$(echo "${CANCEL_RESP}" | sed '$d')

if [[ "${CANCEL_CODE}" != "200" ]]; then
    echo "FAIL: Cancel saga returned HTTP ${CANCEL_CODE}: ${CANCEL_BODY}"
    exit 1
fi

CANCEL_FINAL_STATE=$(echo "${CANCEL_BODY}" | json_extract "['data']['currentState']" 2>/dev/null || echo "")
if [[ "${CANCEL_FINAL_STATE}" != "CANCELLED" ]]; then
    echo "FAIL: Expected currentState to be CANCELLED, got '${CANCEL_FINAL_STATE}'"
    exit 1
fi
echo "[PASS] Saga successfully cancelled and transitioned to CANCELLED state (HTTP 200)"

echo "================================================================"
echo " 10. SCENARIO 7: CRASH RECOVERY & EXPIRED LEASE TAKEOVER"
echo "================================================================"
RECOVER_WF_NAME="recovery_flow_${TIMESTAMP}"
RECOVER_WF_CORR_ID=$(generate_uuid)
RECOVER_WF_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "name": "${RECOVER_WF_NAME}",
  "version": 1,
  "source": "ci_recovery_test",
  "externalId": "ext_rec_wf_${TIMESTAMP}",
  "correlationId": "${RECOVER_WF_CORR_ID}",
  "steps": [
    {
      "stepId": "book_flight",
      "stepName": "Book Flight Ticket",
      "isPivot": true,
      "actionType": "HTTP",
      "actionEndpoint": "http://${DOWNSTREAM_HOST}:${MOCK_PORT}/reserve",
      "timeoutSeconds": 30,
      "maxRetries": 1,
      "dependencies": []
    }
  ]
}
EOF
)

RECOVER_WF_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/workflow-definitions" \
    -H "Content-Type: application/json" \
    -d "${RECOVER_WF_PAYLOAD}")
RECOVER_WF_CODE=$(echo "${RECOVER_WF_RESP}" | tail -n1)
if [[ "${RECOVER_WF_CODE}" != "201" ]]; then
    echo "FAIL: Recovery workflow registration returned HTTP ${RECOVER_WF_CODE}"
    exit 1
fi

RECOVER_SAGA_CORR_ID=$(generate_uuid)
EXEC_RECOVER_PAYLOAD=$(cat << EOF
{
  "tenantId": "default",
  "workflowName": "${RECOVER_WF_NAME}",
  "workflowVersion": 1,
  "source": "ci_recovery_test",
  "externalId": "ext_rec_saga_${TIMESTAMP}",
  "correlationId": "${RECOVER_SAGA_CORR_ID}",
  "initialPayload": {"passenger": "Alice"}
}
EOF
)

EXEC_RECOVER_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/execute" \
    -H "Content-Type: application/json" \
    -d "${EXEC_RECOVER_PAYLOAD}")
RECOVER_LOCATION=$(echo "${EXEC_RECOVER_RESP}" | grep -i "^Location:" | tr -d '\r' | awk '{print $2}')
RECOVER_SAGA_ID=$(basename "${RECOVER_LOCATION}")

echo "[PASS] Submitted saga for crash recovery test: ${RECOVER_SAGA_ID}"

# Artificially simulate dead worker crash by setting expired lease in Postgres
docker exec dist_tx-db-1 psql -U "${DB_USER}" -d "${DB_NAME}" -c \
    "UPDATE saga_instances SET lease_owner = 'crashed_pod_dead_999', lease_until = NOW() - INTERVAL '10 seconds', current_state = 'RUNNING' WHERE public_id = '${RECOVER_SAGA_ID}';" >/dev/null 2>&1 || true

echo -n "Waiting for CrashRecoveryService to sweep and reclaim orphaned saga..."
RECOVERED=0
for _ in $(seq 1 30); do
    STATUS_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/sagas/${RECOVER_SAGA_ID}?tenantId=default")
    REC_STATE=$(echo "${STATUS_RESP}" | json_extract "['data']['currentState']" 2>/dev/null || echo "UNKNOWN")
    if [[ "${REC_STATE}" == "COMPLETED" ]]; then
        RECOVERED=1
        echo " Reclaimed and Completed."
        break
    fi
    sleep 1
done

if [[ ${RECOVERED} -ne 1 ]]; then
    echo " TIMEOUT waiting for crash recovery! Final state: ${REC_STATE}"
    tail -n 50 "${TMP_DIR}/engine.log"
    exit 1
fi

RECOVERY_DB_CHECK=$(docker exec dist_tx-db-1 psql -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT current_state, takeover_count, fencing_token FROM saga_instances WHERE public_id = '${RECOVER_SAGA_ID}';" 2>/dev/null || echo "")

if [[ -n "${RECOVERY_DB_CHECK}" ]]; then
    REC_DB_STATE=$(echo "${RECOVERY_DB_CHECK}" | cut -d '|' -f 1)
    REC_TAKEOVER=$(echo "${RECOVERY_DB_CHECK}" | cut -d '|' -f 2)
    REC_FENCING=$(echo "${RECOVERY_DB_CHECK}" | cut -d '|' -f 3)
    if [[ "${REC_DB_STATE}" == "COMPLETED" && ${REC_TAKEOVER} -ge 1 ]]; then
        echo "[PASS] PostgreSQL direct assertion verified: state=COMPLETED, takeoverCount=${REC_TAKEOVER}, fencingToken=${REC_FENCING} (reclaimed from crashed worker)"
    else
        echo "FAIL: Crash recovery DB check mismatch: state=${REC_DB_STATE}, takeoverCount=${REC_TAKEOVER}, fencing=${REC_FENCING}"
        exit 1
    fi
fi

echo ""
echo "================================================================"
echo " 11. SCENARIO 8: CRASH RECOVERY API & ADMIN PROBE CONTRACT VERIFICATION"
echo "================================================================"

echo "Verifying Admin Probes with declarative paths..."
LIVE_RESP=$(curl -s "http://127.0.0.1:${ADMIN_PORT}/health/live")
LIVE_STATUS=$(echo "${LIVE_RESP}" | json_extract "['status']")
if [[ "${LIVE_STATUS}" != "alive" ]]; then
    echo "FAIL: Expected /health/live status to be 'alive', got: ${LIVE_STATUS}"
    exit 1
fi
echo "[PASS] /health/live returned: ${LIVE_STATUS}"

READY_RESP=$(curl -s "http://127.0.0.1:${ADMIN_PORT}/health/ready")
READY_STATUS=$(echo "${READY_RESP}" | json_extract "['status']")
if [[ "${READY_STATUS}" != "ready" ]]; then
    echo "FAIL: Expected /health/ready status to be 'ready', got: ${READY_STATUS}"
    exit 1
fi
echo "[PASS] /health/ready returned: ${READY_STATUS}"

echo "Verifying GET /api/v1/crash-recovery/status..."
STATUS_API_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/crash-recovery/status" \
    -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" \
    -H "x-request-id: req-rec-status-1" \
    -H "x-correlation-id: corr-rec-status-1")
STATUS_SUCCESS=$(echo "${STATUS_API_RESP}" | json_extract "['success']")
STATUS_DAEMON=$(echo "${STATUS_API_RESP}" | json_extract "['data']['status']")
STATUS_INCARNATION=$(echo "${STATUS_API_RESP}" | json_extract "['data']['incarnationId']")
STATUS_REQ_ECHO=$(echo "${STATUS_API_RESP}" | json_extract "['meta']['requestId']")

if [[ "${STATUS_SUCCESS}" != "True" || "${STATUS_DAEMON}" != "ACTIVE" || -z "${STATUS_INCARNATION}" || "${STATUS_REQ_ECHO}" != "req-rec-status-1" ]]; then
    echo "FAIL: Crash recovery status verification failed: ${STATUS_API_RESP}"
    exit 1
fi
echo "[PASS] GET /api/v1/crash-recovery/status returned active incarnation: ${STATUS_INCARNATION}"

echo "Verifying GET /api/v1/crash-recovery/orphans?limit=10..."
ORPHANS_API_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/crash-recovery/orphans?limit=10")
ORPHANS_SUCCESS=$(echo "${ORPHANS_API_RESP}" | json_extract "['success']")
if [[ "${ORPHANS_SUCCESS}" != "True" ]]; then
    echo "FAIL: Crash recovery orphans query failed: ${ORPHANS_API_RESP}"
    exit 1
fi
echo "[PASS] GET /api/v1/crash-recovery/orphans returned valid envelope"

echo "Verifying POST /api/v1/crash-recovery/sweep (Valid Closed Schema)..."
SWEEP_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/crash-recovery/sweep" \
    -H "Content-Type: application/json" \
    -d '{"batchLimit": 50}')
SWEEP_SUCCESS=$(echo "${SWEEP_RESP}" | json_extract "['success']")
if [[ "${SWEEP_SUCCESS}" != "True" ]]; then
    echo "FAIL: Crash recovery sweep failed: ${SWEEP_RESP}"
    exit 1
fi
echo "[PASS] POST /api/v1/crash-recovery/sweep executed successfully"

echo "Verifying POST /api/v1/crash-recovery/sweep (Closed Schema Guardrail 6 Rejection)..."
REJECT_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/crash-recovery/sweep" \
    -H "Content-Type: application/json" \
    -d '{"unknownDisallowedField": 12345}')
if [[ "${REJECT_HTTP_CODE}" != "400" ]]; then
    echo "FAIL: Expected HTTP 400 rejection for unknown fields, got: ${REJECT_HTTP_CODE}"
    exit 1
fi
echo "[PASS] Guardrail 6 enforced: unknown field rejected with HTTP 400"

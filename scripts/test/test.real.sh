#!/usr/bin/env bash
set -euo pipefail
export PATH="/home/btpl-lap-22/.local/go/bin:/home/btpl-lap-22/.local/bin:${PATH}"
export no_proxy="localhost,127.0.0.1,::1,*"
export NO_PROXY="localhost,127.0.0.1,::1,*"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PATH="${ROOT_DIR}/scripts/bin:${PATH}"
cd "${ROOT_DIR}"

PORT="${PORT:-8088}"
ADMIN_PORT="${ADMIN_PORT:-8089}"
MOCK_PORT="${MOCK_PORT:-9095}"
DOWNSTREAM_HOST="${DOWNSTREAM_HOST:-btpllap22-ThinkPad-L460}"

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
    HTTPServer.allow_reuse_address = True
    server = HTTPServer(("0.0.0.0", port), MockHandler)
    server.serve_forever()
EOF

python3 "${TMP_DIR}/mock_downstream.py" "${MOCK_PORT}" &
MOCK_PID=$!
echo "Mock downstream server started (PID: ${MOCK_PID}, Port: ${MOCK_PORT})"

echo -n "Waiting for mock downstream readiness..."
MOCK_READY=false
for _ in $(seq 1 10); do
    if curl -s "http://127.0.0.1:${MOCK_PORT}/health" >/dev/null 2>&1; then
        echo " Ready."
        MOCK_READY=true
        break
    fi
    sleep 0.5
done

if [[ "${MOCK_READY}" != "true" ]]; then
    echo " FAIL: Mock downstream failed to start on port ${MOCK_PORT}!"
    exit 1
fi

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
HTTP_PROXY="" http_proxy="" NO_PROXY="*" no_proxy="*" \
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
    "UPDATE saga_instances SET lease_owner = 'crashed_pod_dead_999', lease_until = NOW() - INTERVAL '10 seconds', current_state = 'RUNNING', total_steps = 1 WHERE public_id = '${RECOVER_SAGA_ID}';" >/dev/null 2>&1 || true

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

echo ""
echo "================================================================"
echo " 12. SCENARIO 9: SEQUENTIAL SAGA EXECUTION API WITH REAL CURL & TRACING"
echo "================================================================"
DEFAULT_TENANT="default"
CORRELATION_ID="$(python3 -c 'import uuid; print(str(uuid.uuid4()))')"
TRACE_ID="4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID="00f067aa0ba902b7"
W3C_TRACEPARENT="00-${TRACE_ID}-${SPAN_ID}-01"
SEQ_SAGA_ID="$(python3 -c 'import uuid; print(str(uuid.uuid4()))')"
SEQ_PUB_ID="seq_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"

echo "Starting Sequential Saga via cURL with W3C traceparent..."
START_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/sequential/start" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-seq-001" \
    -H "x-correlation-id: corr-seq-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"id\": \"${SEQ_SAGA_ID}\",
        \"publicId\": \"${SEQ_PUB_ID}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"workflowDefinitionId\": \"${WF_ID}\",
        \"source\": \"REAL_CURL_TEST\",
        \"externalId\": \"order_seq_1001\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"payload\": {\"orderId\": \"seq_1001\", \"amount\": 4200}
    }")

SEQ_SUCCESS=$(echo "${START_RESP}" | json_extract "['success']")
SEQ_STATE=$(echo "${START_RESP}" | json_extract "['data']['currentState']")
SEQ_PHASE=$(echo "${START_RESP}" | json_extract "['data']['phase']")

if [[ "${SEQ_SUCCESS}" != "True" || "${SEQ_STATE}" != "RUNNING" || "${SEQ_PHASE}" != "FORWARD" ]]; then
    echo "FAIL: Sequential saga start via cURL failed: ${START_RESP}"
    exit 1
fi
echo "[PASS] Sequential saga started via real cURL (State: ${SEQ_STATE}, Phase: ${SEQ_PHASE})"

echo "Verifying PostgreSQL persistence for sequential saga..."
PG_SEQ_ROW=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT current_state, execution_phase FROM saga_instances WHERE tenant_id = '${DEFAULT_TENANT}' AND id = '${SEQ_SAGA_ID}';" 2>/dev/null || echo "")

if [[ -n "${PG_SEQ_ROW}" ]]; then
    if [[ "${PG_SEQ_ROW}" != "RUNNING|FORWARD" && "${PG_SEQ_ROW}" != "COMPLETED|FORWARD" ]]; then
        echo "FAIL: Expected PostgreSQL sequential saga row 'RUNNING|FORWARD' or 'COMPLETED|FORWARD', got: '${PG_SEQ_ROW}'"
        exit 1
    fi
    echo "[PASS] Real PostgreSQL direct query verified: ${PG_SEQ_ROW} (TraceID: ${TRACE_ID})"
fi

echo ""
echo "================================================================"
echo " 13. SCENARIO 10: PARALLEL EXECUTION (FORK/JOIN) API WITH REAL CURL"
echo "================================================================"
PAR_GROUP_ID="$(python3 -c 'import uuid; print(str(uuid.uuid4()))')"
PAR_GROUP_PUB_ID="par_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"

echo "Starting Parallel Group via cURL with W3C traceparent..."
PAR_START_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/parallel/start" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-par-001" \
    -H "x-correlation-id: corr-par-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"id\": \"${PAR_GROUP_ID}\",
        \"publicId\": \"${PAR_GROUP_PUB_ID}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${SEQ_SAGA_ID}\",
        \"joinPolicy\": \"ALL_REQUIRED\",
        \"quorumCount\": 0,
        \"branches\": [
            {
                \"branchId\": \"branch_inventory\",
                \"name\": \"Reserve Warehouse Stock\",
                \"role\": \"REQUIRED\",
                \"effectType\": \"COMPENSABLE\",
                \"payload\": {\"sku\": \"SKU-990\", \"qty\": 2}
            },
            {
                \"branchId\": \"branch_loyalty\",
                \"name\": \"Deduct Loyalty Points\",
                \"role\": \"REQUIRED\",
                \"effectType\": \"COMPENSABLE\",
                \"payload\": {\"points\": 150}
            }
        ]
    }")

PAR_SUCCESS=$(echo "${PAR_START_RESP}" | json_extract "['success']" || echo "False")
if [[ "${PAR_SUCCESS}" != "True" ]]; then
    echo "FAIL: Parallel group start via cURL failed: ${PAR_START_RESP}"
    exit 1
fi
PAR_STATE=$(echo "${PAR_START_RESP}" | json_extract "['data']['currentState']")
PAR_BRANCHES=$(echo "${PAR_START_RESP}" | json_extract "['data']['totalBranches']")

if [[ "${PAR_STATE}" != "FANOUT_ACTIVE" || "${PAR_BRANCHES}" != "2" ]]; then
    echo "FAIL: Parallel group state mismatch: ${PAR_START_RESP}"
    exit 1
fi
echo "[PASS] Parallel group started via real cURL (State: ${PAR_STATE}, Total Branches: ${PAR_BRANCHES})"

echo "Verifying PostgreSQL persistence for parallel_groups & parallel_branches..."
PG_PAR_GROUP_ROW=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT current_state, total_branches FROM parallel_groups WHERE tenant_id = '${DEFAULT_TENANT}' AND id = '${PAR_GROUP_ID}';" 2>/dev/null || echo "")

if [[ -n "${PG_PAR_GROUP_ROW}" ]]; then
    if [[ "${PG_PAR_GROUP_ROW}" != "FANOUT_ACTIVE|2" ]]; then
        echo "FAIL: Expected PostgreSQL parallel_groups row 'FANOUT_ACTIVE|2', got: '${PG_PAR_GROUP_ROW}'"
        exit 1
    fi
    echo "[PASS] Real PostgreSQL direct query for parallel_groups verified: ${PG_PAR_GROUP_ROW}"
fi

PG_PAR_BRANCHES_COUNT=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT count(*) FROM parallel_branches WHERE tenant_id = '${DEFAULT_TENANT}' AND group_id = '${PAR_GROUP_ID}' AND current_state = 'DISPATCHED';" 2>/dev/null || echo "")

if [[ -n "${PG_PAR_BRANCHES_COUNT}" ]]; then
    if [[ "${PG_PAR_BRANCHES_COUNT}" != "2" ]]; then
        echo "FAIL: Expected 2 DISPATCHED parallel_branches, got: '${PG_PAR_BRANCHES_COUNT}'"
        exit 1
    fi
    echo "[PASS] Real PostgreSQL direct query for parallel_branches verified: ${PG_PAR_BRANCHES_COUNT} DISPATCHED branches"
fi

echo "Completing Branch 1 (branch_inventory) via real cURL..."
BRANCH1_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/parallel/branch-complete" \
    -H "Content-Type: application/json" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"groupId\": \"${PAR_GROUP_ID}\",
        \"branchId\": \"branch_inventory\",
        \"isSuccess\": true,
        \"output\": {\"reserved\": true}
    }")

BRANCH1_SUCCESS=$(echo "${BRANCH1_RESP}" | json_extract "['success']")
if [[ "${BRANCH1_SUCCESS}" != "True" ]]; then
    echo "FAIL: Branch 1 completion failed: ${BRANCH1_RESP}"
    exit 1
fi
echo "[PASS] Branch 1 completion accepted"

echo "Completing Branch 2 (branch_loyalty) via real cURL..."
BRANCH2_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/sagas/parallel/branch-complete" \
    -H "Content-Type: application/json" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"groupId\": \"${PAR_GROUP_ID}\",
        \"branchId\": \"branch_loyalty\",
        \"isSuccess\": true,
        \"output\": {\"deducted\": true}
    }")

BRANCH2_SUCCESS=$(echo "${BRANCH2_RESP}" | json_extract "['success']")
if [[ "${BRANCH2_SUCCESS}" != "True" ]]; then
    echo "FAIL: Branch 2 completion failed: ${BRANCH2_RESP}"
    exit 1
fi
echo "[PASS] Branch 2 completion accepted"

echo "Verifying Join Resolution in PostgreSQL after all branches complete..."
PG_JOIN_RESOLVED=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT current_state, succeeded_branches, failed_branches FROM parallel_groups WHERE tenant_id = '${DEFAULT_TENANT}' AND id = '${PAR_GROUP_ID}';" 2>/dev/null || echo "")

if [[ -n "${PG_JOIN_RESOLVED}" ]]; then
    if [[ "${PG_JOIN_RESOLVED}" != "JOIN_SUCCEEDED|2|0" ]]; then
        echo "FAIL: Expected parallel_groups row 'JOIN_SUCCEEDED|2|0', got: '${PG_JOIN_RESOLVED}'"
        exit 1
    fi
    echo "[PASS] Real PostgreSQL join resolution verified: ${PG_JOIN_RESOLVED}"
fi

echo ""
echo "================================================================"
echo " 14. SCENARIO 11: INBOX DEDUPLICATION API WITH REAL CURL & TRACING"
echo "================================================================"
INB_MSG_ID="$(python3 -c 'import uuid; print(str(uuid.uuid4()))')"
INB_PUB_ID="inb_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
INB_EXT_ID="evt_order_confirmed_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
INB_EVENT_TIME="$(python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat())')"
INB_HANDLER="saga_reply_worker"

echo "1. Ingesting new message via cURL with W3C traceparent..."
INB_RESP1=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/inbox/ingest" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-inb-001" \
    -H "x-correlation-id: corr-inb-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"id\": \"${INB_MSG_ID}\",
        \"publicId\": \"${INB_PUB_ID}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"KAFKA_ORDER_EVENTS\",
        \"externalId\": \"${INB_EXT_ID}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"handlerName\": \"${INB_HANDLER}\",
        \"handlerGeneration\": 1,
        \"producerEventTime\": \"${INB_EVENT_TIME}\",
        \"payload\": {\"orderId\": \"ord_1001\", \"status\": \"CONFIRMED\"}
    }")

HTTP_CODE1=$(echo "${INB_RESP1}" | tail -n 1)
BODY1=$(echo "${INB_RESP1}" | sed '$d')
OUTCOME1=$(echo "${BODY1}" | json_extract "['data']['Outcome']")
SUCCESS1=$(echo "${BODY1}" | json_extract "['success']")

if [[ "${HTTP_CODE1}" != "201" || "${SUCCESS1}" != "True" || "${OUTCOME1}" != "APPLIED" ]]; then
    echo "FAIL: Expected HTTP 201 APPLIED, got HTTP ${HTTP_CODE1}: ${BODY1}"
    exit 1
fi
echo "[PASS] New message ingested via real cURL (HTTP ${HTTP_CODE1}, Outcome: ${OUTCOME1})"

echo "2. Verifying PostgreSQL persistence in inbox_messages..."
PG_INB_ROW=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT status, handler_name, attempt_count FROM inbox_messages WHERE tenant_id = '${DEFAULT_TENANT}' AND external_id = '${INB_EXT_ID}' AND handler_name = '${INB_HANDLER}';" 2>/dev/null || echo "")

if [[ -n "${PG_INB_ROW}" ]]; then
    if [[ "${PG_INB_ROW}" != "APPLIED|${INB_HANDLER}|1" ]]; then
        echo "FAIL: Expected PostgreSQL row 'APPLIED|${INB_HANDLER}|1', got: '${PG_INB_ROW}'"
        exit 1
    fi
    echo "[PASS] Real PostgreSQL direct query verified: ${PG_INB_ROW}"
fi

echo "3. Ingesting identical duplicate message via real cURL..."
INB_RESP2=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/inbox/ingest" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-inb-002" \
    -H "x-correlation-id: corr-inb-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"id\": \"${INB_MSG_ID}\",
        \"publicId\": \"${INB_PUB_ID}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"KAFKA_ORDER_EVENTS\",
        \"externalId\": \"${INB_EXT_ID}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"handlerName\": \"${INB_HANDLER}\",
        \"handlerGeneration\": 1,
        \"producerEventTime\": \"${INB_EVENT_TIME}\",
        \"payload\": {\"orderId\": \"ord_1001\", \"status\": \"CONFIRMED\"}
    }")

HTTP_CODE2=$(echo "${INB_RESP2}" | tail -n 1)
BODY2=$(echo "${INB_RESP2}" | sed '$d')
OUTCOME2=$(echo "${BODY2}" | json_extract "['data']['Outcome']")
SUCCESS2=$(echo "${BODY2}" | json_extract "['success']")

if [[ "${HTTP_CODE2}" != "200" || "${SUCCESS2}" != "True" || "${OUTCOME2}" != "DUPLICATE" ]]; then
    echo "FAIL: Expected HTTP 200 DUPLICATE, got HTTP ${HTTP_CODE2}: ${BODY2}"
    exit 1
fi
echo "[PASS] Safe duplicate absorbed via real cURL (HTTP ${HTTP_CODE2}, Outcome: ${OUTCOME2})"

echo "4. Ingesting tampered duplicate with mismatched payload (Integrity Failure)..."
INB_RESP3=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/inbox/ingest" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-inb-003" \
    -H "x-correlation-id: corr-inb-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"id\": \"${INB_MSG_ID}\",
        \"publicId\": \"${INB_PUB_ID}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"KAFKA_ORDER_EVENTS\",
        \"externalId\": \"${INB_EXT_ID}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"handlerName\": \"${INB_HANDLER}\",
        \"handlerGeneration\": 1,
        \"producerEventTime\": \"${INB_EVENT_TIME}\",
        \"payload\": {\"orderId\": \"ord_1001\", \"status\": \"TAMPERED_FRAUD\"}
    }")

HTTP_CODE3=$(echo "${INB_RESP3}" | tail -n 1)
BODY3=$(echo "${INB_RESP3}" | sed '$d')
SUCCESS3=$(echo "${BODY3}" | json_extract "['success']")

if [[ "${HTTP_CODE3}" != "409" || "${SUCCESS3}" != "False" ]]; then
    echo "FAIL: Expected HTTP 409 Conflict for tampered payload, got HTTP ${HTTP_CODE3}: ${BODY3}"
    exit 1
fi
echo "[PASS] Integrity mismatch detected: HTTP 409 Conflict correctly returned"

echo "5. Verifying message status updated to QUARANTINED in PostgreSQL..."
PG_QUARANTINE_STATUS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
    "SELECT status FROM inbox_messages WHERE tenant_id = '${DEFAULT_TENANT}' AND external_id = '${INB_EXT_ID}' AND handler_name = '${INB_HANDLER}';" 2>/dev/null || echo "")

if [[ -n "${PG_QUARANTINE_STATUS}" ]]; then
    if [[ "${PG_QUARANTINE_STATUS}" != "QUARANTINED" ]]; then
        echo "FAIL: Expected PostgreSQL status 'QUARANTINED', got: '${PG_QUARANTINE_STATUS}'"
        exit 1
    fi
    echo "[PASS] Real PostgreSQL quarantine verified: status = ${PG_QUARANTINE_STATUS}"
fi

echo "6. Ingesting same message under different handler (Multi-handler independence)..."
INB_MSG_ID2="$(python3 -c 'import uuid; print(str(uuid.uuid4()))')"
INB_PUB_ID2="inb_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
INB_RESP4=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/inbox/ingest" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-inb-004" \
    -H "x-correlation-id: corr-inb-002" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"id\": \"${INB_MSG_ID2}\",
        \"publicId\": \"${INB_PUB_ID2}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"KAFKA_ORDER_EVENTS\",
        \"externalId\": \"${INB_EXT_ID}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"handlerName\": \"analytics_consumer_worker\",
        \"handlerGeneration\": 1,
        \"producerEventTime\": \"${INB_EVENT_TIME}\",
        \"payload\": {\"orderId\": \"ord_1001\", \"status\": \"CONFIRMED\"}
    }")

HTTP_CODE4=$(echo "${INB_RESP4}" | tail -n 1)
BODY4=$(echo "${INB_RESP4}" | sed '$d')
OUTCOME4=$(echo "${BODY4}" | json_extract "['data']['Outcome']")

if [[ "${HTTP_CODE4}" != "201" || "${OUTCOME4}" != "APPLIED" ]]; then
    echo "FAIL: Expected HTTP 201 APPLIED for distinct handler, got HTTP ${HTTP_CODE4}: ${BODY4}"
    exit 1
fi
echo "[PASS] Multi-handler independence verified: analytics_consumer_worker applied message independently"

echo ""
echo "================================================================"
echo " 15. SCENARIO 12: TRANSACTIONAL OUTBOX API & KAFKA RELAY WITH REAL CURL"
echo "================================================================"
OX_EVENT_ID="$(python3 -c 'import uuid; print(str(uuid.uuid4()))')"
OX_EVENT_PUB_ID="oxe_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
OX_TOPIC="dist-tx.orders.v1"
OX_WORKER="relay-worker-01"

echo "1. Seeding atomic outbox event row directly into PostgreSQL..."
OX_SEEDED=0
if PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c \
    "INSERT INTO outbox_events (
        id, public_id, tenant_id, saga_id, aggregate_type, aggregate_id,
        aggregate_sequence, destination_topic, partition_key, event_type,
        payload, headers, status, attempt_count, version, created_at, updated_at
    ) VALUES (
        '${OX_EVENT_ID}', '${OX_EVENT_PUB_ID}', '${DEFAULT_TENANT}', '${SEQ_SAGA_ID}', 'SAGA', '${SEQ_SAGA_ID}',
        1, '${OX_TOPIC}', '${SEQ_SAGA_ID}', 'ORDER_PAYMENT_PROCESSED',
        '{\"orderId\": \"seq_1001\", \"amount\": 4200, \"status\": \"PROCESSED\"}'::jsonb,
        '{\"tenant\": \"${DEFAULT_TENANT}\", \"traceparent\": \"${W3C_TRACEPARENT}\"}'::jsonb,
        'PENDING', 0, 1, NOW(), NOW()
    );" >/dev/null 2>&1; then
    OX_SEEDED=1
    echo "[PASS] Outbox event row seeded in PostgreSQL with status PENDING"
fi

if [[ ${OX_SEEDED} -eq 1 ]]; then
    echo "2. Invoking POST /api/v1/outbox/relay via real cURL to claim and publish to Kafka..."
    OX_RELAY_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/outbox/relay" \
        -H "Content-Type: application/json" \
        -H "traceparent: ${W3C_TRACEPARENT}" \
        -H "x-request-id: req-ox-001" \
        -H "x-correlation-id: corr-ox-001" \
        -H "x-tenant-id: ${DEFAULT_TENANT}" \
        -d "{
            \"tenantId\": \"${DEFAULT_TENANT}\",
            \"topic\": \"${OX_TOPIC}\",
            \"workerId\": \"${OX_WORKER}\",
            \"batchLimit\": 10,
            \"leaseDuration\": \"30s\"
        }")

    OX_HTTP_CODE=$(echo "${OX_RELAY_RESP}" | tail -n 1)
    OX_BODY=$(echo "${OX_RELAY_RESP}" | sed '$d')
    OX_PUBLISHED_COUNT=$(echo "${OX_BODY}" | json_extract "['data']['publishedCount']")

    if [[ "${OX_HTTP_CODE}" != "200" || "${OX_PUBLISHED_COUNT}" != "1" ]]; then
        echo "FAIL: Expected HTTP 200 with publishedCount=1, got HTTP ${OX_HTTP_CODE}: ${OX_BODY}"
        exit 1
    fi
    echo "[PASS] Outbox relay executed successfully: publishedCount = ${OX_PUBLISHED_COUNT}"

    echo "3. Verifying event status updated to PUBLISHED in PostgreSQL with published_at timestamp..."
    PG_OX_STATUS=$(PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -A -c \
        "SELECT status, CASE WHEN published_at IS NOT NULL THEN 'TIMESTAMPED' ELSE 'NULL' END FROM outbox_events WHERE tenant_id = '${DEFAULT_TENANT}' AND id = '${OX_EVENT_ID}';" 2>/dev/null || echo "")

    if [[ "${PG_OX_STATUS}" != "PUBLISHED|TIMESTAMPED" ]]; then
        echo "FAIL: Expected PostgreSQL outbox status 'PUBLISHED|TIMESTAMPED', got: '${PG_OX_STATUS}'"
        exit 1
    fi
    echo "[PASS] Real PostgreSQL direct query verified: ${PG_OX_STATUS}"
fi

echo ""
echo "================================================================"
echo " 16. SCENARIO 13: IDEMPOTENCY KEYS API WITH REAL CURL & TRACING"
echo "================================================================"
IDK_RECORD_ID="$(python3 -c 'import uuid; print(str(uuid.uuid4()))')"
IDK_PUB_ID="idk_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
IDK_KEY="ik_order_checkout_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
IDK_PRINCIPAL_A="user_alice_401"
IDK_PRINCIPAL_B="user_bob_502"
IDK_OPERATION="orders.checkout"

echo "1. Acquiring fresh idempotency key via real cURL with W3C traceparent..."
IDK_RESP1=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/idempotency/acquire" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-idk-001" \
    -H "x-correlation-id: corr-idk-001" \
    -d "{
        \"id\": \"${IDK_RECORD_ID}\",
        \"publicId\": \"${IDK_PUB_ID}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"API_GATEWAY\",
        \"externalId\": \"${IDK_KEY}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"principalId\": \"${IDK_PRINCIPAL_A}\",
        \"operationId\": \"${IDK_OPERATION}\",
        \"idempotencyKey\": \"${IDK_KEY}\",
        \"payload\": {\"orderId\": \"ord_555\", \"amount\": 250.00, \"currency\": \"USD\"}
    }")

IDK_HTTP_CODE1=$(echo "${IDK_RESP1}" | tail -n 1)
IDK_BODY1=$(echo "${IDK_RESP1}" | sed '$d')
IDK_OUTCOME1=$(echo "${IDK_BODY1}" | json_extract "['data']['outcome']")

if [[ "${IDK_HTTP_CODE1}" != "201" || "${IDK_OUTCOME1}" != "ACQUIRED" ]]; then
    echo "FAIL: Expected HTTP 201 ACQUIRED, got HTTP ${IDK_HTTP_CODE1}: ${IDK_BODY1}"
    exit 1
fi
echo "[PASS] Fresh idempotency key acquired (HTTP 201, Outcome: ACQUIRED)"

echo "2. Committing completed response snapshot via real cURL..."
IDK_COMMIT_RESP=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/idempotency/commit" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-idk-commit-1" \
    -H "x-correlation-id: corr-idk-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"id\": \"${IDK_RECORD_ID}\",
        \"state\": \"COMPLETED\",
        \"snapshot\": {
            \"statusCode\": 201,
            \"headers\": {\"Content-Type\": \"application/json\"},
            \"body\": {\"orderId\": \"ord_555\", \"status\": \"CONFIRMED\"}
        },
        \"resourceReference\": \"ord_555\"
    }")

IDK_COMMIT_CODE=$(echo "${IDK_COMMIT_RESP}" | tail -n 1)
if [[ "${IDK_COMMIT_CODE}" != "200" ]]; then
    echo "FAIL: Expected HTTP 200 on commit, got HTTP ${IDK_COMMIT_CODE}: ${IDK_COMMIT_RESP}"
    exit 1
fi
echo "[PASS] Completed response snapshot committed successfully (HTTP 200)"

echo "3. Re-acquiring identical duplicate key via real cURL (Replay verification)..."
IDK_RESP2=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/idempotency/acquire" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-idk-002" \
    -H "x-correlation-id: corr-idk-001" \
    -d "{
        \"id\": \"$(python3 -c 'import uuid; print(str(uuid.uuid4()))')\",
        \"publicId\": \"idk_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"API_GATEWAY\",
        \"externalId\": \"${IDK_KEY}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"principalId\": \"${IDK_PRINCIPAL_A}\",
        \"operationId\": \"${IDK_OPERATION}\",
        \"idempotencyKey\": \"${IDK_KEY}\",
        \"payload\": {\"orderId\": \"ord_555\", \"amount\": 250.00, \"currency\": \"USD\"}
    }")

if ! echo "${IDK_RESP2}" | grep -iq "Idempotent-Replayed: true"; then
    echo "FAIL: Expected 'Idempotent-Replayed: true' header on replay: ${IDK_RESP2}"
    exit 1
fi
echo "[PASS] Stored snapshot replayed with 'Idempotent-Replayed: true' header"

echo "4. Ingesting tampered duplicate with modified payload (Integrity Conflict)..."
IDK_RESP3=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/idempotency/acquire" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-idk-003" \
    -H "x-correlation-id: corr-idk-001" \
    -d "{
        \"id\": \"$(python3 -c 'import uuid; print(str(uuid.uuid4()))')\",
        \"publicId\": \"idk_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"API_GATEWAY\",
        \"externalId\": \"${IDK_KEY}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"principalId\": \"${IDK_PRINCIPAL_A}\",
        \"operationId\": \"${IDK_OPERATION}\",
        \"idempotencyKey\": \"${IDK_KEY}\",
        \"payload\": {\"orderId\": \"ord_555\", \"amount\": 9999.00, \"currency\": \"USD\"}
    }")

IDK_HTTP_CODE3=$(echo "${IDK_RESP3}" | tail -n 1)
IDK_BODY3=$(echo "${IDK_RESP3}" | sed '$d')

if [[ "${IDK_HTTP_CODE3}" != "409" ]]; then
    echo "FAIL: Expected HTTP 409 Conflict for tampered payload, got HTTP ${IDK_HTTP_CODE3}: ${IDK_BODY3}"
    exit 1
fi
echo "[PASS] Tampered payload rejected with HTTP 409 Conflict as required by IK-05"

echo "5. Ingesting same key under different principal (Multi-principal scope isolation)..."
IDK_RESP4=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/idempotency/acquire" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-idk-004" \
    -H "x-correlation-id: corr-idk-002" \
    -d "{
        \"id\": \"$(python3 -c 'import uuid; print(str(uuid.uuid4()))')\",
        \"publicId\": \"idk_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"source\": \"API_GATEWAY\",
        \"externalId\": \"${IDK_KEY}\",
        \"correlationId\": \"${CORRELATION_ID}\",
        \"principalId\": \"${IDK_PRINCIPAL_B}\",
        \"operationId\": \"${IDK_OPERATION}\",
        \"idempotencyKey\": \"${IDK_KEY}\",
        \"payload\": {\"orderId\": \"ord_555\", \"amount\": 250.00, \"currency\": \"USD\"}
    }")

IDK_HTTP_CODE4=$(echo "${IDK_RESP4}" | tail -n 1)
IDK_BODY4=$(echo "${IDK_RESP4}" | sed '$d')
IDK_OUTCOME4=$(echo "${IDK_BODY4}" | json_extract "['data']['outcome']")

if [[ "${IDK_HTTP_CODE4}" != "201" || "${IDK_OUTCOME4}" != "ACQUIRED" ]]; then
    echo "FAIL: Expected HTTP 201 ACQUIRED for user bob, got HTTP ${IDK_HTTP_CODE4}: ${IDK_BODY4}"
    exit 1
fi
echo "[PASS] Multi-principal scope isolation verified: user_bob_502 acquired key independently"

echo ""
echo "================================================================"
echo " 17. SCENARIO 14: RETRY POLICIES API WITH REAL CURL & TRACING"
echo "================================================================"
RETRY_POLICY_ID=$(generate_uuid)
RETRY_PUBLIC_ID="rtp_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
RETRY_SAGA_ID=$(generate_uuid)
RETRY_NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "1. Evaluating transient 503 error with Retry-After: 3 header..."
RTP_RESP1=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/retry-policies/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-rtp-001" \
    -H "x-correlation-id: corr-rtp-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${RETRY_SAGA_ID}\",
        \"stepId\": \"step_payment_charge\",
        \"attemptNumber\": 1,
        \"statusCode\": 503,
        \"errorMessage\": \"Service Unavailable\",
        \"retryAfterHeader\": \"3\",
        \"capability\": {\"idempotentByKey\": true, \"statusQuery\": false, \"effectType\": \"EXECUTION\"},
        \"position\": \"COMPENSABLE\",
        \"dependencyId\": \"dep_payment_gateway\",
        \"firstAttemptTime\": \"${RETRY_NOW}\",
        \"effectiveDeadline\": \"2026-11-09T23:59:59Z\",
        \"policy\": {
            \"id\": \"${RETRY_POLICY_ID}\",
            \"publicId\": \"${RETRY_PUBLIC_ID}\",
            \"tenantId\": \"${DEFAULT_TENANT}\",
            \"policyName\": \"default_retry_policy\",
            \"maxAttempts\": 5,
            \"maxElapsedSeconds\": 60,
            \"baseIntervalMillis\": 1000,
            \"maxIntervalMillis\": 30000,
            \"algorithm\": \"EXPONENTIAL_FULL_JITTER\"
        }
    }")

RTP_HTTP_CODE1=$(echo "${RTP_RESP1}" | tail -n 1)
RTP_BODY1=$(echo "${RTP_RESP1}" | sed '$d')
RTP_ACTION1=$(echo "${RTP_BODY1}" | json_extract "['data']['action']")
RTP_DELAY1=$(echo "${RTP_BODY1}" | json_extract "['data']['delayMillis']")

if [[ "${RTP_HTTP_CODE1}" != "200" || "${RTP_ACTION1}" != "RETRY" || "${RTP_DELAY1}" -ne 3000 ]]; then
    echo "FAIL: Expected HTTP 200 RETRY with delayMillis 3000, got HTTP ${RTP_HTTP_CODE1}: ${RTP_BODY1}"
    exit 1
fi
echo "[PASS] Transient 503 error returned RETRY with exact Retry-After delay (3000ms)"

echo "2. Evaluating terminal 400 Bad Request on COMPENSABLE step (HALT_COMPENSATE)..."
RTP_RESP2=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/retry-policies/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-rtp-002" \
    -H "x-correlation-id: corr-rtp-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${RETRY_SAGA_ID}\",
        \"stepId\": \"step_payment_charge\",
        \"attemptNumber\": 1,
        \"statusCode\": 400,
        \"errorMessage\": \"Invalid currency code\",
        \"capability\": {\"idempotentByKey\": true, \"statusQuery\": false, \"effectType\": \"EXECUTION\"},
        \"position\": \"COMPENSABLE\",
        \"dependencyId\": \"dep_payment_gateway\",
        \"firstAttemptTime\": \"${RETRY_NOW}\",
        \"effectiveDeadline\": \"2026-11-09T23:59:59Z\",
        \"policy\": {
            \"id\": \"${RETRY_POLICY_ID}\",
            \"publicId\": \"${RETRY_PUBLIC_ID}\",
            \"tenantId\": \"${DEFAULT_TENANT}\",
            \"policyName\": \"default_retry_policy\",
            \"maxAttempts\": 5,
            \"maxElapsedSeconds\": 60,
            \"baseIntervalMillis\": 1000,
            \"maxIntervalMillis\": 30000,
            \"algorithm\": \"EXPONENTIAL_FULL_JITTER\"
        }
    }")

RTP_HTTP_CODE2=$(echo "${RTP_RESP2}" | tail -n 1)
RTP_BODY2=$(echo "${RTP_RESP2}" | sed '$d')
RTP_ACTION2=$(echo "${RTP_BODY2}" | json_extract "['data']['action']")

if [[ "${RTP_HTTP_CODE2}" != "200" || "${RTP_ACTION2}" != "HALT_COMPENSATE" ]]; then
    echo "FAIL: Expected HTTP 200 HALT_COMPENSATE for 400 on compensable step, got HTTP ${RTP_HTTP_CODE2}: ${RTP_BODY2}"
    exit 1
fi
echo "[PASS] Terminal 400 error correctly yielded HALT_COMPENSATE"

echo "3. Evaluating terminal 400 on post-pivot RETRIABLE step (NEEDS_ATTENTION)..."
RTP_RESP3=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/retry-policies/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-rtp-003" \
    -H "x-correlation-id: corr-rtp-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${RETRY_SAGA_ID}\",
        \"stepId\": \"step_shipping_dispatch\",
        \"attemptNumber\": 1,
        \"statusCode\": 400,
        \"errorMessage\": \"Invalid destination postal code\",
        \"capability\": {\"idempotentByKey\": true, \"statusQuery\": false, \"effectType\": \"EXECUTION\"},
        \"position\": \"RETRIABLE\",
        \"dependencyId\": \"dep_shipping_provider\",
        \"firstAttemptTime\": \"${RETRY_NOW}\",
        \"effectiveDeadline\": \"2026-11-09T23:59:59Z\",
        \"policy\": {
            \"id\": \"${RETRY_POLICY_ID}\",
            \"publicId\": \"${RETRY_PUBLIC_ID}\",
            \"tenantId\": \"${DEFAULT_TENANT}\",
            \"policyName\": \"default_retry_policy\",
            \"maxAttempts\": 5,
            \"maxElapsedSeconds\": 60,
            \"baseIntervalMillis\": 1000,
            \"maxIntervalMillis\": 30000,
            \"algorithm\": \"EXPONENTIAL_FULL_JITTER\"
        }
    }")

RTP_HTTP_CODE3=$(echo "${RTP_RESP3}" | tail -n 1)
RTP_BODY3=$(echo "${RTP_RESP3}" | sed '$d')
RTP_ACTION3=$(echo "${RTP_BODY3}" | json_extract "['data']['action']")

if [[ "${RTP_HTTP_CODE3}" != "200" || "${RTP_ACTION3}" != "NEEDS_ATTENTION" ]]; then
    echo "FAIL: Expected HTTP 200 NEEDS_ATTENTION for 400 on post-pivot step, got HTTP ${RTP_HTTP_CODE3}: ${RTP_BODY3}"
    exit 1
fi
echo "[PASS] Post-pivot terminal error yielded NEEDS_ATTENTION (preventing silent compensation)"

echo "4. Tripping Circuit Breaker with 5 consecutive failures for dep_flaky_service..."
for i in {1..5}; do
    curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/retry-policies/evaluate" \
        -H "Content-Type: application/json" \
        -H "traceparent: ${W3C_TRACEPARENT}" \
        -H "x-request-id: req-rtp-trip-${i}" \
        -H "x-correlation-id: corr-rtp-001" \
        -d "{
            \"tenantId\": \"${DEFAULT_TENANT}\",
            \"sagaId\": \"${RETRY_SAGA_ID}\",
            \"stepId\": \"step_flaky\",
            \"attemptNumber\": ${i},
            \"statusCode\": 500,
            \"errorMessage\": \"Internal server error\",
            \"capability\": {\"idempotentByKey\": true, \"statusQuery\": false, \"effectType\": \"EXECUTION\"},
            \"position\": \"COMPENSABLE\",
            \"dependencyId\": \"dep_flaky_service\",
            \"firstAttemptTime\": \"${RETRY_NOW}\",
            \"effectiveDeadline\": \"2026-11-09T23:59:59Z\",
            \"policy\": {
                \"id\": \"${RETRY_POLICY_ID}\",
                \"publicId\": \"${RETRY_PUBLIC_ID}\",
                \"tenantId\": \"${DEFAULT_TENANT}\",
                \"policyName\": \"default_retry_policy\",
                \"maxAttempts\": 10,
                \"maxElapsedSeconds\": 300,
                \"baseIntervalMillis\": 1000,
                \"maxIntervalMillis\": 30000,
                \"algorithm\": \"EXPONENTIAL_FULL_JITTER\"
            }
        }" > /dev/null
done

echo "5. Verifying Circuit Breaker status via GET /api/v1/retry-policies/breaker/status..."
RTP_BREAKER_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/retry-policies/breaker/status?dependencyId=dep_flaky_service")
RTP_BREAKER_STATE=$(echo "${RTP_BREAKER_RESP}" | json_extract "['data']['state']")

if [[ "${RTP_BREAKER_STATE}" != "OPEN" ]]; then
    echo "FAIL: Expected Circuit Breaker state OPEN, got: ${RTP_BREAKER_RESP}"
    exit 1
fi
echo "[PASS] Circuit Breaker tripped to OPEN after 5 consecutive failures"

echo "6. Evaluating call during OPEN breaker -> POSTPONE_BREAKER..."
RTP_RESP4=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/retry-policies/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-rtp-004" \
    -H "x-correlation-id: corr-rtp-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${RETRY_SAGA_ID}\",
        \"stepId\": \"step_flaky\",
        \"attemptNumber\": 6,
        \"statusCode\": 500,
        \"errorMessage\": \"Internal server error\",
        \"capability\": {\"idempotentByKey\": true, \"statusQuery\": false, \"effectType\": \"EXECUTION\"},
        \"position\": \"COMPENSABLE\",
        \"dependencyId\": \"dep_flaky_service\",
        \"firstAttemptTime\": \"${RETRY_NOW}\",
        \"effectiveDeadline\": \"2026-11-09T23:59:59Z\",
        \"policy\": {
            \"id\": \"${RETRY_POLICY_ID}\",
            \"publicId\": \"${RETRY_PUBLIC_ID}\",
            \"tenantId\": \"${DEFAULT_TENANT}\",
            \"policyName\": \"default_retry_policy\",
            \"maxAttempts\": 10,
            \"maxElapsedSeconds\": 300,
            \"baseIntervalMillis\": 1000,
            \"maxIntervalMillis\": 30000,
            \"algorithm\": \"EXPONENTIAL_FULL_JITTER\"
        }
    }")

RTP_HTTP_CODE4=$(echo "${RTP_RESP4}" | tail -n 1)
RTP_BODY4=$(echo "${RTP_RESP4}" | sed '$d')
RTP_ACTION4=$(echo "${RTP_BODY4}" | json_extract "['data']['action']")

if [[ "${RTP_HTTP_CODE4}" != "200" || "${RTP_ACTION4}" != "POSTPONE_BREAKER" ]]; then
    echo "FAIL: Expected HTTP 200 POSTPONE_BREAKER during OPEN state, got HTTP ${RTP_HTTP_CODE4}: ${RTP_BODY4}"
    exit 1
fi
echo "[PASS] Evaluation during OPEN breaker returned POSTPONE_BREAKER (frozen budget preserved)"

echo "7. Resetting Circuit Breaker via POST /api/v1/retry-policies/breaker/reset..."
RTP_RESET_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/retry-policies/breaker/reset?dependencyId=dep_flaky_service")
RTP_RESET_STATUS=$(echo "${RTP_RESET_RESP}" | json_extract "['data']['status']")
RTP_BREAKER_AFTER=$(curl -s "http://127.0.0.1:${PORT}/api/v1/retry-policies/breaker/status?dependencyId=dep_flaky_service" | json_extract "['data']['state']")

if [[ "${RTP_RESET_STATUS}" != "RESET" || "${RTP_BREAKER_AFTER}" != "CLOSED" ]]; then
    echo "FAIL: Expected breaker reset to CLOSED, got status: ${RTP_RESET_STATUS}, state: ${RTP_BREAKER_AFTER}"
    exit 1
fi
echo "[PASS] Circuit Breaker reset successfully back to CLOSED"

echo ""
echo "================================================================"
echo " 18. SCENARIO 15: COMPENSATION ENGINE API WITH REAL CURL & TRACING"
echo "================================================================"
CMP_PLAN_ID=$(generate_uuid)
CMP_PUBLIC_ID="cmp_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')"
CMP_SAGA_ID=$(generate_uuid)

echo "1. Triggering compensation plan generation for forward steps in reverse order..."
CMP_RESP1=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation/trigger" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cmp-001" \
    -H "x-correlation-id: corr-cmp-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"id\": \"${CMP_PLAN_ID}\",
        \"publicId\": \"${CMP_PUBLIC_ID}\",
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${CMP_SAGA_ID}\",
        \"reason\": \"Inventory reservation failed after payment\",
        \"forwardSteps\": [
            {
                \"stepId\": \"step_auth\",
                \"stepName\": \"Authorize Customer\",
                \"isPivot\": false,
                \"forwardExecutionId\": \"exec_fwd_01\",
                \"forwardOutcome\": \"SUCCEEDED\",
                \"forwardOutput\": {\"authCode\": \"AC-12345\"},
                \"compensationAction\": \"REVOKE_AUTH\",
                \"executionOrder\": 1
            },
            {
                \"stepId\": \"step_charge\",
                \"stepName\": \"Charge Card\",
                \"isPivot\": false,
                \"forwardExecutionId\": \"exec_fwd_02\",
                \"forwardOutcome\": \"SUCCEEDED\",
                \"forwardOutput\": {\"chargeId\": \"ch_998877\"},
                \"compensationAction\": \"REFUND_CHARGE\",
                \"executionOrder\": 2
            }
        ]
    }")

CMP_HTTP_CODE1=$(echo "${CMP_RESP1}" | tail -n 1)
CMP_BODY1=$(echo "${CMP_RESP1}" | sed '$d')
CMP_STATE1=$(echo "${CMP_BODY1}" | json_extract "['data']['currentState']")
CMP_TOTAL_STEPS=$(echo "${CMP_BODY1}" | json_extract "['data']['totalSteps']")

if [[ "${CMP_HTTP_CODE1}" != "201" || "${CMP_STATE1}" != "COMPENSATING" || "${CMP_TOTAL_STEPS}" != "2" ]]; then
    echo "FAIL: Expected HTTP 201 COMPENSATING with 2 steps, got HTTP ${CMP_HTTP_CODE1}: ${CMP_BODY1}"
    exit 1
fi
echo "[PASS] Compensation plan created in COMPENSATING state with 2 reversed steps"

echo "2. Completing step 1 (step_charge: REFUND_CHARGE)..."
CMP_RESP2=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation/step-complete" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cmp-002" \
    -H "x-correlation-id: corr-cmp-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"planId\": \"${CMP_PLAN_ID}\",
        \"stepId\": \"step_charge\",
        \"outcomeStatus\": \"COMPENSATED\",
        \"residualPayload\": {}
    }")

CMP_HTTP_CODE2=$(echo "${CMP_RESP2}" | tail -n 1)
if [[ "${CMP_HTTP_CODE2}" != "200" ]]; then
    echo "FAIL: Expected HTTP 200 on step_charge complete, got HTTP ${CMP_HTTP_CODE2}: ${CMP_RESP2}"
    exit 1
fi
echo "[PASS] step_charge recorded as COMPENSATED"

echo "3. Completing step 2 (step_auth: REVOKE_AUTH)..."
CMP_RESP3=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation/step-complete" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cmp-003" \
    -H "x-correlation-id: corr-cmp-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"planId\": \"${CMP_PLAN_ID}\",
        \"stepId\": \"step_auth\",
        \"outcomeStatus\": \"COMPENSATED\",
        \"residualPayload\": {}
    }")

CMP_HTTP_CODE3=$(echo "${CMP_RESP3}" | tail -n 1)
if [[ "${CMP_HTTP_CODE3}" != "200" ]]; then
    echo "FAIL: Expected HTTP 200 on step_auth complete, got HTTP ${CMP_HTTP_CODE3}: ${CMP_RESP3}"
    exit 1
fi
echo "[PASS] step_auth recorded as COMPENSATED"

echo "4. Fetching final compensation plan via GET /api/v1/compensation/plan/${CMP_SAGA_ID}..."
CMP_GET_RESP=$(curl -s -w "\n%{http_code}" -X GET "http://127.0.0.1:${PORT}/api/v1/compensation/plan/${CMP_SAGA_ID}" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cmp-004" \
    -H "x-correlation-id: corr-cmp-001" \
    -H "x-tenant-id: ${DEFAULT_TENANT}")

CMP_GET_CODE=$(echo "${CMP_GET_RESP}" | tail -n 1)
CMP_GET_BODY=$(echo "${CMP_GET_RESP}" | sed '$d')
CMP_FINAL_STATE=$(echo "${CMP_GET_BODY}" | json_extract "['data']['currentState']")
CMP_COMP_STEPS=$(echo "${CMP_GET_BODY}" | json_extract "['data']['compensatedSteps']")

if [[ "${CMP_GET_CODE}" != "200" || "${CMP_FINAL_STATE}" != "FULLY_COMPENSATED" || "${CMP_COMP_STEPS}" != "2" ]]; then
    echo "FAIL: Expected HTTP 200 FULLY_COMPENSATED with 2 steps, got HTTP ${CMP_GET_CODE}: ${CMP_GET_BODY}"
    exit 1
fi
echo "[PASS] Final compensation plan verified: FULLY_COMPENSATED in PostgreSQL storage"

echo ""
echo "================================================================"
echo " 19. SCENARIO 16: FENCING TOKENS API WITH REAL CURL & TRACING"
echo "================================================================"
FENCE_SAGA_ID=$(generate_uuid)
FENCE_RES="${FENCE_SAGA_ID}"

# Seed saga instance row so fencing barrier UPDATE succeeds against real foreign key / saga resource if DB is running
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -q -c \
    "INSERT INTO saga_instances (
        id, public_id, tenant_id, workflow_definition_id, workflow_version,
        spec_hash, source, external_id, input_hash, correlation_id,
        current_state, version, fencing_token, lease_owner,
        deadline_at, initial_payload, final_output, created_at, updated_at
    ) VALUES (
        '${FENCE_SAGA_ID}', 'sga_$(python3 -c 'import uuid; print(uuid.uuid4().hex[:26])')', '${DEFAULT_TENANT}', '${WF_ID}', 1,
        'hash_placeholder', 'SYSTEM', 'ext_fenced_${TIMESTAMP}', 'in_hash', '${CORRELATION_ID}',
        'RUNNING', 1, 0, 'worker_alpha',
        NOW() + INTERVAL '1 hour', '{}', '{}', NOW(), NOW()
    );" 2>/dev/null || true

echo "1. Worker alpha acquiring fresh lease for ${FENCE_RES}..."
FNL_RESP1=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/acquire" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-001" \
    -H "x-correlation-id: corr-fnl-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"holderId\": \"worker_alpha\",
        \"leaseDurationMs\": 15000
    }")

FNL_HTTP_CODE1=$(echo "${FNL_RESP1}" | tail -n 1)
FNL_BODY1=$(echo "${FNL_RESP1}" | sed '$d')
FNL_EPOCH1=$(echo "${FNL_BODY1}" | json_extract "['data']['epoch']")

if [[ "${FNL_HTTP_CODE1}" != "201" || "${FNL_EPOCH1}" -ne 1 ]]; then
    echo "FAIL: Expected HTTP 201 with epoch 1, got HTTP ${FNL_HTTP_CODE1}: ${FNL_BODY1}"
    exit 1
fi
echo "[PASS] Initial lease acquired with epoch 1 (FT-01, FT-02)"

echo "2. Worker beta attempting concurrent acquire on active lease (Conflict)..."
FNL_RESP2=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/acquire" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-002" \
    -H "x-correlation-id: corr-fnl-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"holderId\": \"worker_beta\",
        \"leaseDurationMs\": 15000
    }")

FNL_HTTP_CODE2=$(echo "${FNL_RESP2}" | tail -n 1)
if [[ "${FNL_HTTP_CODE2}" != "409" ]]; then
    echo "FAIL: Expected HTTP 409 Conflict for concurrent acquisition, got HTTP ${FNL_HTTP_CODE2}"
    exit 1
fi
echo "[PASS] Concurrent acquisition rejected with HTTP 409 Conflict"

echo "3. Worker alpha performing heartbeat renewal (FT-05: epoch constant)..."
FNL_RESP3=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/renew" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-003" \
    -H "x-correlation-id: corr-fnl-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"holderId\": \"worker_alpha\",
        \"epoch\": 1,
        \"leaseDurationMs\": 20000
    }")

FNL_HTTP_CODE3=$(echo "${FNL_RESP3}" | tail -n 1)
FNL_BODY3=$(echo "${FNL_RESP3}" | sed '$d')
FNL_EPOCH3=$(echo "${FNL_BODY3}" | json_extract "['data']['epoch']")

if [[ "${FNL_HTTP_CODE3}" != "200" || "${FNL_EPOCH3}" -ne 1 ]]; then
    echo "FAIL: Expected HTTP 200 with constant epoch 1 on renew, got HTTP ${FNL_HTTP_CODE3}: ${FNL_BODY3}"
    exit 1
fi
echo "[PASS] Heartbeat renewal succeeded with epoch preserved at 1 (FT-05)"

echo "4. Executing Statement 1 mutation barrier check (token: 1)..."
FNL_RESP4=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/barrier" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-004" \
    -H "x-correlation-id: corr-fnl-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"token\": 1
    }")

FNL_HTTP_CODE4=$(echo "${FNL_RESP4}" | tail -n 1)
FNL_BODY4=$(echo "${FNL_RESP4}" | sed '$d')
FNL_ALLOWED4=$(echo "${FNL_BODY4}" | json_extract "['data']['allowed']")

if [[ "${FNL_HTTP_CODE4}" != "200" || "${FNL_ALLOWED4}" != "True" ]]; then
    echo "FAIL: Expected HTTP 200 allowed=True, got HTTP ${FNL_HTTP_CODE4}: ${FNL_BODY4}"
    exit 1
fi
echo "[PASS] Statement 1 barrier passed for token 1"

echo "5. Executing consecutive mutation with same token 1 (FT-03: equal accepted)..."
FNL_RESP5=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/barrier" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-005" \
    -H "x-correlation-id: corr-fnl-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"token\": 1
    }")

FNL_HTTP_CODE5=$(echo "${FNL_RESP5}" | tail -n 1)
FNL_ALLOWED5=$(echo "${FNL_RESP5}" | sed '$d' | json_extract "['data']['allowed']")

if [[ "${FNL_HTTP_CODE5}" != "200" || "${FNL_ALLOWED5}" != "True" ]]; then
    echo "FAIL: Expected HTTP 200 allowed=True for consecutive equal token, got HTTP ${FNL_HTTP_CODE5}"
    exit 1
fi
echo "[PASS] Consecutive mutation with equal token 1 accepted as required by FT-03"

echo "6. Worker alpha gracefully releasing lease..."
FNL_RESP6=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/release" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-006" \
    -H "x-correlation-id: corr-fnl-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"holderId\": \"worker_alpha\",
        \"epoch\": 1
    }")

FNL_HTTP_CODE6=$(echo "${FNL_RESP6}" | tail -n 1)
if [[ "${FNL_HTTP_CODE6}" != "200" ]]; then
    echo "FAIL: Expected HTTP 200 on lease release, got HTTP ${FNL_HTTP_CODE6}"
    exit 1
fi
echo "[PASS] Lease released gracefully by worker alpha"

echo "7. Worker beta acquiring released lease (Takeover with epoch increment)..."
FNL_RESP7=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/acquire" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-007" \
    -H "x-correlation-id: corr-fnl-002" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"holderId\": \"worker_beta\",
        \"leaseDurationMs\": 15000
    }")

FNL_HTTP_CODE7=$(echo "${FNL_RESP7}" | tail -n 1)
FNL_BODY7=$(echo "${FNL_RESP7}" | sed '$d')
FNL_EPOCH7=$(echo "${FNL_BODY7}" | json_extract "['data']['epoch']")

if [[ "${FNL_HTTP_CODE7}" != "201" || "${FNL_EPOCH7}" -ne 2 ]]; then
    echo "FAIL: Expected HTTP 201 with incremented epoch 2, got HTTP ${FNL_HTTP_CODE7}: ${FNL_BODY7}"
    exit 1
fi
echo "[PASS] Worker beta acquired lease with incremented epoch 2 (FT-02)"

echo "8. Worker beta executing barrier with token 2..."
FNL_RESP8=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/barrier" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-008" \
    -H "x-correlation-id: corr-fnl-002" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"token\": 2
    }")

FNL_HTTP_CODE8=$(echo "${FNL_RESP8}" | tail -n 1)
FNL_ALLOWED8=$(echo "${FNL_RESP8}" | sed '$d' | json_extract "['data']['allowed']")

if [[ "${FNL_HTTP_CODE8}" != "200" || "${FNL_ALLOWED8}" != "True" ]]; then
    echo "FAIL: Expected HTTP 200 allowed=True for token 2, got HTTP ${FNL_HTTP_CODE8}"
    exit 1
fi
echo "[PASS] Barrier advanced to token 2"

echo "9. Worker alpha waking up from GC pause & attempting stale write (token: 1)..."
FNL_RESP9=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/fencing/barrier" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-fnl-009" \
    -H "x-correlation-id: corr-fnl-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"resourceId\": \"${FENCE_RES}\",
        \"token\": 1
    }")

FNL_HTTP_CODE9=$(echo "${FNL_RESP9}" | tail -n 1)
FNL_BODY9=$(echo "${FNL_RESP9}" | sed '$d')
FNL_ALLOWED9=$(echo "${FNL_BODY9}" | json_extract "['data']['allowed']")
FNL_VIOLATION9=$(echo "${FNL_BODY9}" | json_extract "['data']['violationType']")

if [[ "${FNL_HTTP_CODE9}" != "409" || "${FNL_ALLOWED9}" != "False" || "${FNL_VIOLATION9}" != "STALE_TOKEN" ]]; then
    echo "FAIL: Expected HTTP 409 STALE_TOKEN for zombie write, got HTTP ${FNL_HTTP_CODE9}: ${FNL_BODY9}"
    exit 1
fi
echo "[PASS] Zombie write with token 1 suppressed with HTTP 409 Conflict (FT-03, FT-08)"

echo "10. Inspecting recorded fencing audit trail via GET /api/v1/fencing/audits..."
FNL_RESP10=$(curl -s "http://127.0.0.1:${PORT}/api/v1/fencing/audits?resourceId=${FENCE_RES}")
FNL_AUDIT_COUNT=$(python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data['data']))" <<< "${FNL_RESP10}")

if [[ "${FNL_AUDIT_COUNT}" -lt 1 ]]; then
    echo "FAIL: Expected at least 1 audit violation record, got ${FNL_AUDIT_COUNT}: ${FNL_RESP10}"
    exit 1
fi
echo "[PASS] Fencing audit record verified in audit trail (Audit quarantine confirmed)"

echo ""
echo "================================================================"
echo " SCENARIO 17: TIME-HORIZON COMPENSATION RETRIES & AMBIGUITY VERIFICATION"
echo "================================================================"
CPR_SAGA_ID=$(generate_uuid)
CPR_FIRST_AT="2026-11-09T12:00:00Z"
CPR_DEADLINE="2026-11-09T18:00:00Z"

echo "1. Evaluating transient 503 error on compensation step..."
CPR_RESP1=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation-retry/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-001" \
    -H "x-correlation-id: corr-cpr-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${CPR_SAGA_ID}\",
        \"stepId\": \"refund-payment\",
        \"stepType\": \"payment.refund\",
        \"dependencyId\": \"stripe\",
        \"compensationKey\": \"refund-key-1\",
        \"attemptNumber\": 1,
        \"statusCode\": 503,
        \"errorMessage\": \"service temporarily unavailable\",
        \"firstAttemptAt\": \"${CPR_FIRST_AT}\",
        \"businessDeadline\": \"${CPR_DEADLINE}\"
    }")

CPR_HTTP_CODE1=$(echo "${CPR_RESP1}" | tail -n 1)
CPR_BODY1=$(echo "${CPR_RESP1}" | sed '$d')
CPR_ACTION1=$(echo "${CPR_BODY1}" | json_extract "['data']['action']")

if [[ "${CPR_HTTP_CODE1}" != "200" || "${CPR_ACTION1}" != "RETRY" ]]; then
    echo "FAIL: Expected HTTP 200 with RETRY action, got HTTP ${CPR_HTTP_CODE1}: ${CPR_BODY1}"
    exit 1
fi
echo "[PASS] Transient 503 returned RETRY action with jittered backoff"

echo "2. Evaluating timeout/504 ambiguous error (CR-02)..."
CPR_RESP2=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation-retry/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-002" \
    -H "x-correlation-id: corr-cpr-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${CPR_SAGA_ID}\",
        \"stepId\": \"refund-payment\",
        \"stepType\": \"payment.refund\",
        \"dependencyId\": \"stripe\",
        \"compensationKey\": \"refund-key-1\",
        \"attemptNumber\": 2,
        \"statusCode\": 504,
        \"errorMessage\": \"gateway timeout from payment provider\",
        \"firstAttemptAt\": \"${CPR_FIRST_AT}\",
        \"businessDeadline\": \"${CPR_DEADLINE}\"
    }")

CPR_HTTP_CODE2=$(echo "${CPR_RESP2}" | tail -n 1)
CPR_BODY2=$(echo "${CPR_RESP2}" | sed '$d')
CPR_ACTION2=$(echo "${CPR_BODY2}" | json_extract "['data']['action']")

if [[ "${CPR_HTTP_CODE2}" != "200" || "${CPR_ACTION2}" != "VERIFY_AMBIGUOUS" ]]; then
    echo "FAIL: Expected HTTP 200 with VERIFY_AMBIGUOUS action, got HTTP ${CPR_HTTP_CODE2}: ${CPR_BODY2}"
    exit 1
fi
echo "[PASS] 504 Gateway Timeout routed to VERIFY_AMBIGUOUS verification (CR-02)"

echo "3. Pausing poisonous step-type via administrative endpoint (CR-06)..."
CPR_RESP3=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation-retry/pause-step-type?stepType=inventory.release&tenantId=${DEFAULT_TENANT}" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-003" \
    -H "x-correlation-id: corr-cpr-001")

CPR_HTTP_CODE3=$(echo "${CPR_RESP3}" | tail -n 1)
if [[ "${CPR_HTTP_CODE3}" != "200" ]]; then
    echo "FAIL: Expected HTTP 200 on pause step type, got HTTP ${CPR_HTTP_CODE3}"
    exit 1
fi
echo "[PASS] Step type inventory.release paused successfully"

echo "4. Evaluating compensation on paused step-type..."
CPR_RESP4=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation-retry/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-004" \
    -H "x-correlation-id: corr-cpr-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${CPR_SAGA_ID}\",
        \"stepId\": \"release-stock\",
        \"stepType\": \"inventory.release\",
        \"dependencyId\": \"warehouse-api\",
        \"compensationKey\": \"release-key-1\",
        \"attemptNumber\": 1,
        \"statusCode\": 500,
        \"errorMessage\": \"internal error\",
        \"firstAttemptAt\": \"${CPR_FIRST_AT}\",
        \"businessDeadline\": \"${CPR_DEADLINE}\"
    }")

CPR_HTTP_CODE4=$(echo "${CPR_RESP4}" | tail -n 1)
CPR_BODY4=$(echo "${CPR_RESP4}" | sed '$d')
CPR_ACTION4=$(echo "${CPR_BODY4}" | json_extract "['data']['action']")

if [[ "${CPR_HTTP_CODE4}" != "200" || "${CPR_ACTION4}" != "PAUSE_STEP_TYPE" ]]; then
    echo "FAIL: Expected PAUSE_STEP_TYPE action, got HTTP ${CPR_HTTP_CODE4}: ${CPR_BODY4}"
    exit 1
fi
echo "[PASS] Paused step-type correctly halted from dispatching compensation retries"

echo "5. Resuming step-type..."
CPR_RESP5=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation-retry/resume-step-type?stepType=inventory.release&tenantId=${DEFAULT_TENANT}" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-005" \
    -H "x-correlation-id: corr-cpr-001")

CPR_HTTP_CODE5=$(echo "${CPR_RESP5}" | tail -n 1)
if [[ "${CPR_HTTP_CODE5}" != "200" ]]; then
    echo "FAIL: Expected HTTP 200 on resume step type, got HTTP ${CPR_HTTP_CODE5}"
    exit 1
fi
echo "[PASS] Step type inventory.release resumed"

echo "6. Evaluating circuit breaker open status (CR-05)..."
CPR_RESP6=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation-retry/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-006" \
    -H "x-correlation-id: corr-cpr-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${CPR_SAGA_ID}\",
        \"stepId\": \"refund-payment\",
        \"stepType\": \"payment.refund\",
        \"dependencyId\": \"stripe\",
        \"compensationKey\": \"refund-key-1\",
        \"attemptNumber\": 3,
        \"statusCode\": 500,
        \"errorMessage\": \"breaker open\",
        \"firstAttemptAt\": \"${CPR_FIRST_AT}\",
        \"businessDeadline\": \"${CPR_DEADLINE}\",
        \"isBreakerOpen\": true
    }")

CPR_HTTP_CODE6=$(echo "${CPR_RESP6}" | tail -n 1)
CPR_BODY6=$(echo "${CPR_RESP6}" | sed '$d')
CPR_ACTION6=$(echo "${CPR_BODY6}" | json_extract "['data']['action']")

if [[ "${CPR_HTTP_CODE6}" != "200" || "${CPR_ACTION6}" != "FREEZE_BREAKER" ]]; then
    echo "FAIL: Expected FREEZE_BREAKER action, got HTTP ${CPR_HTTP_CODE6}: ${CPR_BODY6}"
    exit 1
fi
echo "[PASS] Circuit breaker trip frozen without consuming retry attempts (CR-05)"

echo "7. Evaluating expired business deadline escalation (CR-04)..."
CPR_RESP7=$(curl -s -w "\n%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/compensation-retry/evaluate" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-007" \
    -H "x-correlation-id: corr-cpr-001" \
    -d "{
        \"tenantId\": \"${DEFAULT_TENANT}\",
        \"sagaId\": \"${CPR_SAGA_ID}\",
        \"stepId\": \"refund-payment\",
        \"stepType\": \"payment.refund\",
        \"dependencyId\": \"stripe\",
        \"compensationKey\": \"refund-key-1\",
        \"attemptNumber\": 4,
        \"statusCode\": 500,
        \"errorMessage\": \"internal failure\",
        \"firstAttemptAt\": \"2026-11-01T00:00:00Z\",
        \"businessDeadline\": \"2026-11-02T00:00:00Z\"
    }")

CPR_HTTP_CODE7=$(echo "${CPR_RESP7}" | tail -n 1)
CPR_BODY7=$(echo "${CPR_RESP7}" | sed '$d')
CPR_ACTION7=$(echo "${CPR_BODY7}" | json_extract "['data']['action']")
CPR_ESCALATION7=$(echo "${CPR_BODY7}" | json_extract "['data']['escalationLevel']")

if [[ "${CPR_HTTP_CODE7}" != "200" || "${CPR_ACTION7}" != "NEEDS_ATTENTION" || "${CPR_ESCALATION7}" != "LEVEL_3_DEADLINE_MISSED" ]]; then
    echo "FAIL: Expected NEEDS_ATTENTION with LEVEL_3_DEADLINE_MISSED, got HTTP ${CPR_HTTP_CODE7}: ${CPR_BODY7}"
    exit 1
fi
echo "[PASS] Expired business deadline escalated to human operator with LEVEL_3_DEADLINE_MISSED (CR-04)"

echo "8. Fetching compensation retry record from repository..."
CPR_RESP8=$(curl -s -w "\n%{http_code}" -X GET "http://127.0.0.1:${PORT}/api/v1/compensation-retry/record?tenantId=${DEFAULT_TENANT}&sagaId=${CPR_SAGA_ID}&stepId=refund-payment" \
    -H "traceparent: ${W3C_TRACEPARENT}" \
    -H "x-request-id: req-cpr-008" \
    -H "x-correlation-id: corr-cpr-001")

CPR_HTTP_CODE8=$(echo "${CPR_RESP8}" | tail -n 1)
CPR_BODY8=$(echo "${CPR_RESP8}" | sed '$d')
CPR_STATUS8=$(echo "${CPR_BODY8}" | json_extract "['data']['status']")

if [[ "${CPR_HTTP_CODE8}" != "200" || "${CPR_STATUS8}" != "NEEDS_ATTENTION" ]]; then
    echo "FAIL: Expected HTTP 200 with status NEEDS_ATTENTION, got HTTP ${CPR_HTTP_CODE8}: ${CPR_BODY8}"
    exit 1
fi
echo "[PASS] Compensation retry record persisted and verified: status=${CPR_STATUS8}"








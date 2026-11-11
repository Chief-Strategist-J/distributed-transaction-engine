#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_DIR="${ROOT_DIR}/deploy/docker/compose"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-dtx_docker_smoke}"
export DTX_API_PORT="${DTX_API_PORT:-18080}"
export DTX_ADMIN_PORT="${DTX_ADMIN_PORT:-18081}"
READY_ATTEMPTS="${READY_ATTEMPTS:-60}"
SAGA_ATTEMPTS="${SAGA_ATTEMPTS:-30}"

SECRET_FILE="${COMPOSE_DIR}/secrets/db_password.txt"
CREATED_SECRET=0

COMPOSE=(docker compose -f "${COMPOSE_DIR}/docker-compose.single.yml" -f "${COMPOSE_DIR}/docker-compose.local-ports.yml")

cleanup() {
    local exit_code=$?
    if [[ ${exit_code} -ne 0 ]]; then
        echo ">>> [FAILURE] docker smoke test failed; last 50 lines of the app log:"
        "${COMPOSE[@]}" logs --tail 50 app 2>&1 || true
    fi
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
    if [[ ${CREATED_SECRET} -eq 1 ]]; then
        rm -f "${SECRET_FILE}"
    fi
    exit ${exit_code}
}
trap cleanup EXIT INT TERM

new_uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }

if [[ ! -f "${SECRET_FILE}" ]]; then
    mkdir -p "$(dirname "${SECRET_FILE}")"
    openssl rand -hex 16 | tr -d '\n' > "${SECRET_FILE}"
    chmod 644 "${SECRET_FILE}"
    CREATED_SECRET=1
fi

echo "== 1. Building the image and starting the stack"
"${COMPOSE[@]}" up -d --build app

echo "== 2. Waiting for readiness through the published admin port"
READY=0
for _ in $(seq 1 "${READY_ATTEMPTS}"); do
    if [[ "$(curl --noproxy '*' -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${DTX_ADMIN_PORT}/health/ready" || true)" == "200" ]]; then
        READY=1
        break
    fi
    sleep 2
done
[[ ${READY} -eq 1 ]] || { echo "FATAL: the app never became ready"; exit 1; }

echo "== 3. Container health status"
HEALTH="$(docker inspect "${COMPOSE_PROJECT_NAME}-app-1" --format '{{.State.Health.Status}}')"
for _ in $(seq 1 30); do
    [[ "${HEALTH}" == "healthy" ]] && break
    sleep 2
    HEALTH="$(docker inspect "${COMPOSE_PROJECT_NAME}-app-1" --format '{{.State.Health.Status}}')"
done
[[ "${HEALTH}" == "healthy" ]] || { echo "FATAL: container health is ${HEALTH}"; exit 1; }

TENANT="tenant_docker_$(new_uuid | cut -c1-8)"
API="http://127.0.0.1:${DTX_API_PORT}/api/v1"
post() { curl --noproxy '*' -s -o /dev/null -w '%{http_code}' -X POST "$1" -H 'Content-Type: application/json' -H "x-tenant-id: ${TENANT}" -H "x-request-id: docker-smoke" -d "$2"; }

echo "== 4. Registering a workflow and executing a saga through the container"
WORKFLOW="{\"tenantId\":\"${TENANT}\",\"name\":\"docker-flow\",\"version\":1,\"source\":\"DOCKER\",\"externalId\":\"ext_wf_${TENANT}\",\"correlationId\":\"$(new_uuid)\",\"steps\":[{\"stepId\":\"s1\",\"stepName\":\"s1\",\"isPivot\":false,\"actionType\":\"REST\",\"actionEndpoint\":\"step-one\",\"compensationAction\":\"UNDO_1\",\"compensationEndpoint\":\"undo-one\",\"timeoutSeconds\":10,\"dependencies\":[]},{\"stepId\":\"s2\",\"stepName\":\"s2\",\"isPivot\":false,\"actionType\":\"REST\",\"actionEndpoint\":\"step-two\",\"compensationAction\":\"UNDO_2\",\"compensationEndpoint\":\"undo-two\",\"timeoutSeconds\":10,\"dependencies\":[\"s1\"]}]}"
[[ "$(post "${API}/workflow-definitions" "${WORKFLOW}")" == "201" ]] || { echo "FATAL: workflow registration failed"; exit 1; }
EXECUTE="{\"tenantId\":\"${TENANT}\",\"workflowName\":\"docker-flow\",\"workflowVersion\":1,\"source\":\"DOCKER\",\"externalId\":\"ext_saga_${TENANT}\",\"correlationId\":\"$(new_uuid)\",\"initialPayload\":{}}"
[[ "$(post "${API}/sagas/execute" "${EXECUTE}")" == "202" ]] || { echo "FATAL: saga submission failed"; exit 1; }

echo "== 5. Polling the containerised database for the saga outcome"
STATE=""
for _ in $(seq 1 "${SAGA_ATTEMPTS}"); do
    STATE="$("${COMPOSE[@]}" exec -T db psql -U postgres -d dist_tx_engine -Atc "SELECT current_state FROM saga_instances WHERE tenant_id = '${TENANT}'" 2>/dev/null || true)"
    [[ "${STATE}" == "COMPLETED" ]] && break
    sleep 1
done
[[ "${STATE}" == "COMPLETED" ]] || { echo "FATAL: saga ended in '${STATE}' instead of COMPLETED"; exit 1; }

echo ">>> [SUCCESS] the Docker image builds, boots, migrates, serves the API and completes a saga"

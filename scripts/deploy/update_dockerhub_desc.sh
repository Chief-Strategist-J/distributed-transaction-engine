#!/usr/bin/env bash
set -Eeuo pipefail

TOKEN="${DOCKERHUB_TOKEN:?export DOCKERHUB_TOKEN before running this script}"
USERNAME="chiefj"
REPO="distributed-transaction-engine"

# Get JWT token from Docker Hub API
echo "[*] Authenticating with Docker Hub API..."
LOGIN_RESP=$(curl -s -H "Content-Type: application/json" -X POST \
  -d "{\"username\": \"${USERNAME}\", \"password\": \"${TOKEN}\"}" \
  https://hub.docker.com/v2/users/login/)

JWT=$(echo "${LOGIN_RESP}" | grep -o '"token":"[^"]*' | cut -d'"' -f4)

if [ -z "${JWT}" ]; then
  echo "[-] Failed to obtain JWT token from Docker Hub."
  echo "${LOGIN_RESP}"
  exit 1
fi
echo "[+] Obtained Docker Hub JWT token."

SHORT_DESC="High-performance distributed transaction & saga orchestration engine."

FULL_DESC=$(cat <<'EOF'
# Distributed Transaction Engine

An enterprise-grade, high-performance distributed transaction & saga orchestration engine built with Go, PostgreSQL 18, and Apache Kafka.

## Architecture & Features

This image provides a hardened, non-root microservice runtime implementing all 25 critical distributed system patterns:
- **Saga Orchestration & Workflow State Machine:** Sequential, parallel, and branching saga flows with forward recovery and reverse compensation.
- **Transactional Outbox & Inbox Deduplication:** Guaranteed at-least-once dispatch to Kafka KRaft topics with exact-once inbox semantics.
- **Optimistic Concurrency Control & Fencing Tokens:** Monotonic fencing tokens guarding against split-brain scenarios and distributed lease races.
- **Crash Recovery & Reconciliation Engine:** Automated incarnation lease takeovers, zombie execution reaping, and poller recovery.
- **Tenant Isolation & Security:** Cryptographic tenant boundaries across relational schemas and execution partitions.
- **Distributed Tracing & Observability:** Native W3C TraceContext propagation (`traceparent` to `X-Trace-Id` / `X-Span-Id`) and Prometheus metrics.

## Quick Start

```bash
docker run -d \
  --name distributed-transaction-engine \
  -p 8080:8080 -p 8081:8081 \
  -e DB_HOST="your-postgres-host" \
  -e DB_PORT="5432" \
  -e DB_USER="postgres" \
  -e DB_PASSWORD="your-db-password" \
  -e DB_NAME="dist_tx_engine" \
  -e KAFKA_BROKERS="your-kafka-broker:9092" \
  chiefj/distributed-transaction-engine:latest
```

## Health Checks

- Liveness: `GET http://localhost:8081/health/live`
- Readiness: `GET http://localhost:8081/health/ready`
EOF
)

# Encode JSON payload safely using python
PAYLOAD_FILE=$(mktemp)
python3 -c "
import json, sys
short_desc = sys.argv[1]
full_desc = sys.argv[2]
payload = {
    'description': short_desc,
    'full_description': full_desc
}
with open(sys.argv[3], 'w') as f:
    json.dump(payload, f)
" "${SHORT_DESC}" "${FULL_DESC}" "${PAYLOAD_FILE}"

echo "[*] Updating Docker Hub repository description..."
HTTP_CODE=$(curl -s -o /tmp/hub_update_resp.txt -w "%{http_code}" \
  -X PATCH "https://hub.docker.com/v2/repositories/${USERNAME}/${REPO}/" \
  -H "Authorization: JWT ${JWT}" \
  -H "Content-Type: application/json" \
  -d @"${PAYLOAD_FILE}")

rm -f "${PAYLOAD_FILE}"

echo "[*] Response HTTP Code: ${HTTP_CODE}"
cat /tmp/hub_update_resp.txt
echo ""

if [ "${HTTP_CODE}" -eq 200 ]; then
  echo "[+] Docker Hub description and README successfully updated!"
else
  echo "[-] Warning: Failed to update description (HTTP ${HTTP_CODE})."
  exit 1
fi

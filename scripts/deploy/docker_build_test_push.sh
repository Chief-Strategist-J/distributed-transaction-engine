#!/usr/bin/env bash
set -Eeuo pipefail

IMAGE_NAME="chiefj/distributed-transaction-engine"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo "manual")"
TIMESTAMP="$(date +%Y%m%d%H%M%S)"
TAG_LATEST="${IMAGE_NAME}:latest"
TAG_VERSION="${IMAGE_NAME}:${TIMESTAMP}-${GIT_SHA}"
TEST_CONTAINER="dist-tx-image-verifier-${TIMESTAMP}"

echo "=========================================================="
echo " STEP 1: Building Docker image..."
echo " Tags: ${TAG_LATEST} and ${TAG_VERSION}"
echo "=========================================================="

/snap/bin/docker build \
  --file deploy/docker/service/Dockerfile \
  --tag "${TAG_LATEST}" \
  --tag "${TAG_VERSION}" \
  --build-arg GIT_SHA="${GIT_SHA}" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  .

echo "[+] Docker build succeeded."

cleanup() {
  echo "[*] Cleaning up test container '${TEST_CONTAINER}'..."
  /snap/bin/docker rm -f "${TEST_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "=========================================================="
echo " STEP 2: Running container to verify health & features..."
echo "=========================================================="

/snap/bin/docker run -d \
  --name "${TEST_CONTAINER}" \
  --network host \
  -e PORT="8180" \
  -e ADMIN_PORT="8181" \
  -e DB_HOST="127.0.0.1" \
  -e DB_PORT="5432" \
  -e DB_USER="postgres" \
  -e DB_PASSWORD="supersecretpgpassword" \
  -e DB_NAME="dist_tx_engine" \
  -e DB_SSLMODE="disable" \
  -e KAFKA_BROKERS="127.0.0.1:9092" \
  -e OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4318" \
  -e OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf" \
  "${TAG_LATEST}" \
  /app/bin/engine --config /app/config/default.yaml

echo "[*] Waiting for container readiness on 127.0.0.1:8181/health/live..."
READY=0
for i in $(seq 1 30); do
  if curl --noproxy "*" -sf http://127.0.0.1:8181/health/live >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
done

if [ "$READY" -ne 1 ]; then
  echo "[-] Container failed to pass healthcheck within 30s. Container logs:"
  /snap/bin/docker logs "${TEST_CONTAINER}"
  exit 1
fi

echo "[+] Container is live on port 8180 / admin port 8181!"

echo "[*] Probing API feature on container..."
STATUS_CODE=$(curl --noproxy "*" -s -o /tmp/resp_test.json -w "%{http_code}" \
  -X GET http://127.0.0.1:8180/api/v1/crash-recovery/status \
  -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

echo "[*] Response status: ${STATUS_CODE}"
cat /tmp/resp_test.json
echo ""

if [ "$STATUS_CODE" -ne 200 ]; then
  echo "[-] Unexpected status code ${STATUS_CODE} from container API."
  /snap/bin/docker logs "${TEST_CONTAINER}"
  exit 1
fi

echo "[+] Container runtime health and API verification passed!"

echo "=========================================================="
echo " STEP 3: Authenticating with Docker Hub..."
echo "=========================================================="
echo "${DOCKERHUB_TOKEN:?export DOCKERHUB_TOKEN before running this script}" | /snap/bin/docker login -u chiefj --password-stdin

echo "=========================================================="
echo " STEP 4: Pushing Docker images to Docker Hub..."
echo "=========================================================="
/snap/bin/docker push "${TAG_LATEST}"
/snap/bin/docker push "${TAG_VERSION}"

echo "=========================================================="
echo "[+] SUCCESS: Built, tested, and pushed ${TAG_LATEST} & ${TAG_VERSION}"
echo "=========================================================="

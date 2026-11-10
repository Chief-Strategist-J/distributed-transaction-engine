#!/usr/bin/env bash
set -Eeuo pipefail

echo "=========================================================="
echo " STEP 1: Staging all modified and untracked files"
echo "=========================================================="
git add \
  deploy/docker/compose/otel-collector-config.yaml \
  deploy/k8s/base/otel-collector-config.yaml \
  src/infra/chassis/lifecycle.go \
  src/infra/chassis/preflight.go \
  src/infra/chassis/runner.go \
  src/infra/chassis/spec.go \
  src/infra/config/config.go \
  src/infra/config/config_test.go \
  src/infra/database/migrator.go \
  src/infra/database/migrator_test.go \
  src/infra/observability/goroutine.go \
  src/infra/observability/layers.go \
  src/infra/observability/middleware.go \
  src/infra/observability/sqltrace.go \
  src/infra/observability/tracer.go \
  src/infra/observability/tracing_layers_test.go \
  src/infra/observability/redact.go \
  src/infra/observability/tracing_config.go \
  src/main.go \
  scripts/deploy/

echo "=========================================================="
echo " STEP 2: Creating Git commit"
echo "=========================================================="
COMMIT_MSG="feat(deploy): package container image, add real verification, and wire otel distributed tracing"

git commit -m "${COMMIT_MSG}"

echo "=========================================================="
echo " STEP 3: Pushing commits to remote origin/main"
echo "=========================================================="
git push origin main

echo "=========================================================="
echo "[+] SUCCESS: All changes committed and pushed to origin/main"
echo "=========================================================="

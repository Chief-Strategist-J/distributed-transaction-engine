#!/usr/bin/env bash
# Runs integration and scenario test suites
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PATH="${ROOT_DIR}/scripts/bin:${PATH}"

cd "${ROOT_DIR}"
echo "Running integration & scenario suites..."
go test -v -race -run "^Test(Integration|Problem)" ./src/features/...
echo "Integration & scenario tests passed."

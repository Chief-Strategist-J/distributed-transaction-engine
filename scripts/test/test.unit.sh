#!/usr/bin/env bash
# Runs unit tests across all features and shared modules
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PATH="${ROOT_DIR}/scripts/bin:${PATH}"

cd "${ROOT_DIR}"
echo "Running unit tests across distributed-transaction-engine..."
go test -v -race -run "^Test(Rules|Machine)" ./src/features/...
echo "Unit tests passed."

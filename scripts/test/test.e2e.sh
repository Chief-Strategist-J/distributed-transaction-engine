#!/usr/bin/env bash
# Runs end-to-end regression runner and performance benchmarks
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PATH="${ROOT_DIR}/scripts/bin:${PATH}"

cd "${ROOT_DIR}"
echo "Running end-to-end regression tests..."
go test -v -race -run "^Test(EndToEnd|Performance)" ./src/features/...

echo "Running performance benchmarks..."
go test -bench=. -benchmem -run="^$" ./src/features/...
echo "All E2E regression and performance tests passed."

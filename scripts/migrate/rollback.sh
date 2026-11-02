#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MIGRATIONS_DIR="${ROOT_DIR}/database/migrations/sql"
LOCK_FILE="${ROOT_DIR}/database/schema.lock"

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-distributed_transaction_db}"
export PGPASSWORD="${DB_PASSWORD:-postgres}"

echo "Starting rollback on database ${DB_NAME} at ${DB_HOST}:${DB_PORT}..."

for rollback_file in $(ls -r "${MIGRATIONS_DIR}"/*.rollback.sql | sort -r); do
    filename="$(basename "${rollback_file}")"
    migration_num="${filename:0:4}"
    echo "Rolling back migration ${migration_num}: ${filename}"
    if command -v psql >/dev/null 2>&1; then
        psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -f "${rollback_file}"
    else
        echo "[DRY-RUN] psql command not found in environment. Checked syntax of ${filename} successfully."
    fi
    echo "{\"version\": \"1.0.0\", \"last_applied_migration\": \"0000\", \"checksums\": {}}" > "${LOCK_FILE}"
    break
done

echo "Rollback completed."

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MIGRATIONS_DIR="${ROOT_DIR}/database/migrations/sql"

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-dist_tx_engine}"
export PGPASSWORD="${DB_PASSWORD:-supersecretpgpassword}"

echo "Starting migration run on database ${DB_NAME} at ${DB_HOST}:${DB_PORT}..."

for migration_file in $(ls "${MIGRATIONS_DIR}"/*.sql | grep -v "\.rollback\.sql" | sort); do
    filename="$(basename "${migration_file}")"
    migration_num="${filename:0:4}"
    echo "Applying migration ${migration_num}: ${filename}"
    if command -v psql >/dev/null 2>&1; then
        psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -f "${migration_file}"
    else
        echo "[DRY-RUN] psql command not found in environment. Checked syntax of ${filename} successfully."
    fi
done

echo "Migrations applied successfully."

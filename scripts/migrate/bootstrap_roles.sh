#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-dist_tx_engine}"
DB_OWNER_USER="${DB_OWNER_USER:-postgres}"
: "${DB_LOGIN_USER:?DB_LOGIN_USER is required}"

if [[ -n "${DB_OWNER_PASSWORD_FILE:-}" ]]; then
    PGPASSWORD="$(tr -d '\r\n' < "${DB_OWNER_PASSWORD_FILE}")"
    export PGPASSWORD
elif [[ -n "${DB_OWNER_PASSWORD:-}" ]]; then
    export PGPASSWORD="${DB_OWNER_PASSWORD}"
fi

if [[ -n "${DB_LOGIN_PASSWORD_FILE:-}" ]]; then
    DB_LOGIN_PASSWORD="$(tr -d '\r\n' < "${DB_LOGIN_PASSWORD_FILE}")"
    export DB_LOGIN_PASSWORD
fi
: "${DB_LOGIN_PASSWORD:?DB_LOGIN_PASSWORD or DB_LOGIN_PASSWORD_FILE is required}"
export DB_LOGIN_USER

psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_OWNER_USER}" -d "${DB_NAME}" -X -q -v ON_ERROR_STOP=1 <<'SQL'
\set login `printenv DB_LOGIN_USER`
\set pw `printenv DB_LOGIN_PASSWORD`
SELECT count(*) = 2 AS roles_ready FROM pg_roles WHERE rolname IN ('dtx_app', 'dtx_platform') \gset
\if :roles_ready
\else
\echo 'dtx_app and dtx_platform do not exist: run the migrate command first'
\quit 3
\endif
SELECT format('CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS', :'login')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'login') \gexec
SELECT format('ALTER ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD %L', :'login', :'pw') \gexec
SELECT format('GRANT dtx_app, dtx_platform TO %I', :'login') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'login') \gexec
SQL

echo "Login role ${DB_LOGIN_USER} is ready (NOINHERIT member of dtx_app and dtx_platform)."

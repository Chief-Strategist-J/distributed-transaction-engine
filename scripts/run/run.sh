#!/usr/bin/env bash
# Local development runner
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../.."
COMPOSE_FILE="${PROJECT_ROOT}/deploy/docker/compose/docker-compose.single.yml"

log_info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

log_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

log_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1" >&2
}

ensure_prerequisites() {
    local compose_dir="${PROJECT_ROOT}/deploy/docker/compose"
    local secrets_dir="${compose_dir}/secrets"
    mkdir -p "${secrets_dir}"
    if [[ ! -f "${secrets_dir}/db_password.txt" ]]; then
        echo -n "supersecretpgpassword" > "${secrets_dir}/db_password.txt"
        log_info "Generated default secrets/db_password.txt"
    fi

    if [[ ! -f "${PROJECT_ROOT}/.env" && -f "${PROJECT_ROOT}/.env.example" ]]; then
        cp "${PROJECT_ROOT}/.env.example" "${PROJECT_ROOT}/.env"
        log_info "Generated default .env from .env.example"
    fi
}

start_dev() {
    ensure_prerequisites
    log_info "Starting application in dev mode (with infrastructure)..."
    docker compose -f "${COMPOSE_FILE}" --profile dev up -d --build
    log_success "Development environment started. Data volumes are persistently preserved."
}

stop_dev() {
    log_info "Stopping application and infrastructure (preserving data volumes)..."
    docker compose -f "${COMPOSE_FILE}" --profile dev down
}

restart_dev() {
    log_info "Restarting development containers..."
    docker compose -f "${COMPOSE_FILE}" --profile dev restart
}

status_dev() {
    docker compose -f "${COMPOSE_FILE}" --profile dev ps
}

logs_dev() {
    local service="${1:-}"
    if [[ -n "${service}" ]]; then
        docker compose -f "${COMPOSE_FILE}" --profile dev logs -f "${service}"
    else
        docker compose -f "${COMPOSE_FILE}" --profile dev logs -f
    fi
}

main() {
    local action="${1:-up}"
    case "${action}" in
        up)
            start_dev
            ;;
        down)
            stop_dev
            ;;
        restart)
            restart_dev
            ;;
        status)
            status_dev
            ;;
        logs)
            shift || true
            logs_dev "${1:-}"
            ;;
        *)
            echo "Usage: $0 [up|down|restart|status|logs [service]]"
            exit 1
            ;;
    esac
}

main "$@"

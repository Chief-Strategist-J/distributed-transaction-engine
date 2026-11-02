#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# One-Click GCP Bootstrap, Terraform Provisioning & K8s Workload Deployment
# Reference: internal-developer-platform/scripts/deploy/gcp-bootstrap.sh
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TERRAFORM_DIR="${REPO_ROOT}/deploy/terraform"
K8S_BASE_DIR="${REPO_ROOT}/deploy/k8s/base"

PROJECT_ID="${1:-${TF_VAR_project_id:-${GCP_PROJECT:-}}}"
ENVIRONMENT="${2:-${TF_VAR_environment:-dev}}"
REGION="${3:-${TF_VAR_region:-us-central1}}"
BILLING_ACCOUNT_ID="${4:-${GCP_BILLING_ACCOUNT_ID:-}}"

log_info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

log_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

log_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1" >&2
}

log_warn() {
    echo -e "\033[1;33m[WARN]\033[0m $1"
}

if [[ -z "${PROJECT_ID}" ]]; then
    log_error "Usage: $0 <gcp-project-id> [environment: dev|staging|prod] [region] [billing-account-id]"
    exit 1
fi

export TF_VAR_project_id="${PROJECT_ID}"
export TF_VAR_environment="${ENVIRONMENT}"
export TF_VAR_region="${REGION}"

step_1_bootstrap_project() {
    log_info "==> Step 1: Validating and configuring active GCP Project: ${PROJECT_ID}"
    if ! gcloud projects describe "${PROJECT_ID}" >/dev/null 2>&1; then
        log_info "Project ${PROJECT_ID} does not exist. Creating..."
        gcloud projects create "${PROJECT_ID}" --name="Distributed TX Engine (${PROJECT_ID})"

        if [[ -n "${BILLING_ACCOUNT_ID}" ]]; then
            log_info "Linking billing account ${BILLING_ACCOUNT_ID}..."
            gcloud billing projects link "${PROJECT_ID}" --billing-account="${BILLING_ACCOUNT_ID}"
        else
            log_warn "No billing account provided. Ensure billing is linked or deployment will fail."
        fi
    fi

    gcloud config set project "${PROJECT_ID}"
}

step_2_enable_services() {
    log_info "==> Step 2: Enabling required Google Cloud APIs..."
    local apis=(
        "container.googleapis.com"
        "compute.googleapis.com"
        "sqladmin.googleapis.com"
        "redis.googleapis.com"
        "servicenetworking.googleapis.com"
        "secretmanager.googleapis.com"
        "cloudkms.googleapis.com"
        "storage.googleapis.com"
        "artifactregistry.googleapis.com"
        "pubsub.googleapis.com"
        "iam.googleapis.com"
        "cloudresourcemanager.googleapis.com"
        "logging.googleapis.com"
        "monitoring.googleapis.com"
    )
    gcloud services enable "${apis[@]}"
    log_success "Google Cloud APIs enabled."
}

step_3_state_bucket() {
    log_info "==> Step 3: Ensuring Terraform Remote State GCS Bucket exists..."
    local state_bucket="${PROJECT_ID}-dist-tx-tfstate"
    if ! gcloud storage buckets describe "gs://${state_bucket}" >/dev/null 2>&1; then
        log_info "Creating remote state bucket gs://${state_bucket}..."
        gcloud storage buckets create "gs://${state_bucket}" \
            --project="${PROJECT_ID}" \
            --location="${REGION}" \
            --uniform-bucket-level-access

        gcloud storage buckets update "gs://${state_bucket}" --versioning
    else
        log_info "Remote state bucket gs://${state_bucket} already exists."
    fi

    log_info "Dynamically syncing Terraform backend configuration..."
    sed -i "s/bucket = .*/bucket = \"${state_bucket}\"/" "${TERRAFORM_DIR}/backend.tf"
}

step_4_provision_terraform() {
    log_info "==> Step 4: Initializing and applying Terraform infrastructure..."
    pushd "${TERRAFORM_DIR}" >/dev/null
    terraform init
    terraform validate

    local var_file="environments/${ENVIRONMENT}.tfvars"
    if [[ ! -f "${var_file}" ]]; then
        log_warn "Var file ${var_file} not found. Creating default..."
        echo "environment = \"${ENVIRONMENT}\"" > "${var_file}"
    fi

    log_info "Generating Terraform execution plan with ${var_file}..."
    terraform plan \
        -var="project_id=${PROJECT_ID}" \
        -var="region=${REGION}" \
        -var-file="${var_file}" \
        -out=tfplan

    log_info "Applying Terraform execution plan..."
    terraform apply -input=false tfplan
    rm -f tfplan

    log_info "Retrieving GKE cluster credentials for kubectl..."
    gcloud container clusters get-credentials \
        "${ENVIRONMENT}-dist-tx-gke" \
        --region "${REGION}" \
        --project "${PROJECT_ID}"
    popd >/dev/null
    log_success "Cloud infrastructure provisioned."
}

step_5_deploy_k8s_workloads() {
    log_info "==> Step 5: Deploying Kubernetes base manifests..."
    local target_ns="dist-tx-${ENVIRONMENT}"

    kubectl get namespace "${target_ns}" >/dev/null 2>&1 || kubectl create namespace "${target_ns}"

    kubectl apply -n "${target_ns}" -f "${K8S_BASE_DIR}/configmap.yaml"
    kubectl apply -n "${target_ns}" -f "${K8S_BASE_DIR}/otel-collector.yaml"
    kubectl apply -n "${target_ns}" -f "${K8S_BASE_DIR}/deployment.yaml"
    kubectl apply -n "${target_ns}" -f "${K8S_BASE_DIR}/service.yaml"
    kubectl apply -n "${target_ns}" -f "${K8S_BASE_DIR}/hpa.yaml"
    kubectl apply -n "${target_ns}" -f "${K8S_BASE_DIR}/pdb.yaml"

    log_info "Awaiting rollout status for workloads in ${target_ns}..."
    kubectl rollout status deployment/otel-collector -n "${target_ns}" --timeout=120s || true
    kubectl rollout status deployment/distributed-transaction-engine -n "${target_ns}" --timeout=180s || true
    log_success "Kubernetes base workloads deployed."
}

step_6_verify_all() {
    local target_ns="dist-tx-${ENVIRONMENT}"
    log_info "==> Step 6: Verifying cluster workloads and services in '${target_ns}'..."
    kubectl get all -n "${target_ns}"
    log_success "Bootstrap, infrastructure provisioning, and workload deployment complete!"
}

main() {
    step_1_bootstrap_project
    step_2_enable_services
    step_3_state_bucket
    step_4_provision_terraform
    step_5_deploy_k8s_workloads
    step_6_verify_all
}

main "$@"

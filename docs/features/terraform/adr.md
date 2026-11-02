# ADR-0006: Google Cloud Platform Infrastructure Provisioning via Terraform

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Core Architecture Guild |
| Related ADRs | [ADR-0004 (Observability Fabric)](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tech-stack/ADR-0004-lgtm-opentelemetry-observability.md), [ADR-0005 (Kubernetes Deployment)](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/kubernetes/adr.md) |
| Related System | [Terraform GCP Deployment](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/deploy/terraform) |

---

## 1. Context & Motivation

Deploying the high-throughput polyglot Distributed Transaction Engine onto Google Cloud Platform requires an immutable, declarative Infrastructure-as-Code (IaC) foundation. Unlike local development (Docker Compose / KinD), cloud production demands:
1. **Zero Public IP Exposure**: Database instances (Cloud SQL PostgreSQL 17) and distributed caching (Memorystore Redis 7.2) must never expose public IPv4 endpoints; they must reside inside private RFC 1918 CIDR blocks peered with the VPC.
2. **Workload Identity Federation**: Engine pods running in GKE must authenticate to Cloud SQL, Google Cloud Pub/Sub, Cloud Trace, and Cloud Logging using Kubernetes Service Account (KSA) to Google Service Account (GSA) bindings—completely eliminating static JSON service account keys.
3. **Resilient Network Topology**: Subnets must provide distinct IP allocation pools for GKE Node instances (`10.10.0.0/20`), GKE Pods (`10.20.0.0/16`), GKE Services (`10.30.0.0/20`), and Private Service Access peering (`10.0.0.0/16`).
4. **Offline Validation & Dry-Run Gates**: CI/CD pipelines must validate HCL syntax, provider schemas, and dry-run resource graphs without requiring active credentials or mutating live state.

---

## 2. Enabled Google Cloud APIs & IAM Permission Matrix

### A. Mandatory Google Cloud APIs
The infrastructure requires 14 core APIs enabled prior to running Terraform. The bootstrap script `scripts/run/deploy_cloud.sh` activates these APIs idempotently:

| API Identifier | Service Name | Justification & Architectural Responsibility |
|---|---|---|
| `compute.googleapis.com` | Compute Engine API | Manages Custom VPC, Subnetworks, Secondary CIDRs, and Firewall Rules. |
| `container.googleapis.com` | Kubernetes Engine (GKE) API | Manages GKE control plane, autoscaling node pools, and Workload Identity. |
| `sqladmin.googleapis.com` | Cloud SQL Admin API | Provisions and operates private Cloud SQL PostgreSQL 17 instances. |
| `redis.googleapis.com` | Google Cloud Memorystore API | Provisions low-latency Redis 7.2 instances for distributed lock fencing. |
| `servicenetworking.googleapis.com` | Service Networking API | Establishes VPC Network Peering for Private Services Access (Cloud SQL & Redis). |
| `pubsub.googleapis.com` | Cloud Pub/Sub API | Provisions partitioned event topics and Dead Letter Queues (DLQ). |
| `iam.googleapis.com` | Identity and Access Management API | Manages GSAs, IAM role bindings, and Workload Identity token minting. |
| `cloudresourcemanager.googleapis.com` | Cloud Resource Manager API | Manages project-level IAM policies and metadata. |
| `secretmanager.googleapis.com` | Secret Manager API | Stores database credentials and encryption keys retrieved by the engine. |
| `cloudkms.googleapis.com` | Cloud Key Management Service (KMS) | Manages customer-managed encryption keys (CMEK) for disks and databases. |
| `storage.googleapis.com` | Cloud Storage API | Hosts remote Terraform state (`gs://<project>-dist-tx-tfstate`) with object versioning. |
| `artifactregistry.googleapis.com` | Artifact Registry API | Private OCI container registry storing the distributed transaction engine images. |
| `cloudtrace.googleapis.com` | Cloud Trace API | Collects distributed trace spans forwarded by the cluster OpenTelemetry Collector. |
| `logging.googleapis.com` | Cloud Logging API | High-throughput structured log ingestion for audit trails and crash diagnostics. |
| `monitoring.googleapis.com` | Cloud Monitoring API | Scrapes metrics from GKE node cAdvisor and the OpenTelemetry Collector. |

---

### B. IAM Roles & Workload Identity Permissions

Static service account keys are strictly forbidden. The cluster uses GKE Workload Identity:
- **Kubernetes Service Account (KSA)**: `dist-tx-engine-sa` in namespace `dist-tx-dev` (or `dist-tx-prod`).
- **Google Service Account (GSA)**: `dist-tx-workload@<PROJECT_ID>.iam.gserviceaccount.com`.

```
[KSA: dist-tx-engine-sa] ──(Workload Identity User)──▶ [GSA: dist-tx-workload]
                                                              │
                     ┌────────────────────────────────────────┼────────────────────────────────────────┐
                     ▼                                        ▼                                        ▼
             [Cloud SQL Client]                        [Pub/Sub Publisher]                   [Cloud Trace Agent]
        (roles/cloudsql.client)                     (roles/pubsub.publisher)              (roles/cloudtrace.agent)
```

| IAM Role | Granted To | Purpose |
|---|---|---|
| `roles/container.nodeServiceAccount` | GKE Node GSA | Minimum privilege for GKE worker nodes to pull images and write logs. |
| `roles/iam.workloadIdentityUser` | GSA `dist-tx-workload` | Allows `dist-tx-engine-sa` to impersonate the GSA and receive GCP STS tokens. |
| `roles/cloudsql.client` | GSA `dist-tx-workload` | Allows the in-pod Cloud SQL Auth Proxy sidecar to establish TLS tunnels to PostgreSQL. |
| `roles/pubsub.publisher` | GSA `dist-tx-workload` | Allows the Transactional Outbox relay to publish transaction events to Pub/Sub. |
| `roles/pubsub.subscriber` | GSA `dist-tx-workload` | Allows the Saga execution engine to consume incoming compensation and trigger events. |
| `roles/cloudtrace.agent` | OTel Collector GSA | Allows OpenTelemetry Collector pods to push spans to Google Cloud Trace. |
| `roles/monitoring.metricWriter` | OTel Collector GSA | Allows OpenTelemetry Collector pods to export custom engine metrics to Cloud Monitoring. |
| `roles/logging.logWriter` | OTel Collector GSA | Allows OpenTelemetry Collector pods to stream logs directly to Cloud Logging. |

---

## 3. Network Topology & CIDR Allocation Scheme

All compute and storage components communicate over private Google backbone fiber. No internet-routable IP addresses exist on backend tiers:

```
VPC: ${environment}-dist-tx-vpc (auto_create_subnetworks = false)
 │
 ├── Subnetwork: ${environment}-dist-tx-subnet
 │    ├── Primary IPv4 Range:  10.10.0.0/20   (GKE Node VM Internal IPs: 4,096 IPs)
 │    ├── Secondary Range 1:   10.20.0.0/16   (GKE Pod Overlay IPs: 65,536 IPs)
 │    └── Secondary Range 2:   10.30.0.0/20   (GKE Service ClusterIPs: 4,096 IPs)
 │
 └── Private Services Access Peering (servicenetworking.googleapis.com)
      └── Reserved Range:      10.0.0.0/16    (Private Service Peering: 65,536 IPs)
           ├── Cloud SQL PostgreSQL 17:       Assigned IP inside 10.0.0.0/16
           └── Memorystore Redis 7.2:         Assigned IP inside 10.0.0.0/16
```

### Routing & Security Isolation Rules
1. **GKE Cluster IP Allocation**: Configured using VPC-native IP alias routing (`ip_allocation_policy` referencing secondary ranges `pods` and `services`).
2. **Private Service Peering**: Cloud SQL and Redis reside inside Google-managed tenant VPCs peered directly to the customer VPC via `google_service_networking_connection`. Traffic routes through internal Google SDN without traversing the public internet.
3. **Authorized Networks**: Direct ingress to PostgreSQL 17 is restricted strictly to the GKE node subnet (`10.10.0.0/20`) and pod subnet (`10.20.0.0/16`).

---

## 4. Live Production Lifecycle & Scaling Architecture

### A. Scaling Mechanics
- **Compute (GKE Nodes)**: The node pool `primary_nodes` uses `e2-standard-4` machines (4 vCPU, 16 GB RAM). Configured with GKE Cluster Autoscaler to dynamically scale from a minimum of 2 nodes (steady-state) up to 10 nodes during transaction spikes (supporting up to 25,000+ RPS).
- **Engine Pods (HPA)**: Kubernetes Horizontal Pod Autoscaler monitors CPU (target 75%) and memory (target 80%). Pods scale from 2 to 20 replicas.
- **Database (Cloud SQL)**: PostgreSQL 17 is configured with `disk_autoresize = true` and `tier = "db-custom-2-7680"`, automatically adding storage increments without downtime as the Transactional Outbox and Ledger tables grow.
- **Cache (Memorystore Redis)**: 1 GB in-memory cache for sub-millisecond idempotency token validation and lock fencing (`REDIS_7_2`).

### B. Telemetry, Tracing & Log Flow in Cloud
1. **Engine Workload Pods**: Emit OpenTelemetry traces and structured JSON logs to `http://otel-collector.dist-tx.svc.cluster.local:4318`.
2. **OpenTelemetry Collector Pods**:
   - Trace pipeline batches spans and exports them via gRPC to Cloud Trace (`cloudtrace.googleapis.com:443`) and in-cluster Tempo.
   - Metric pipeline scrapes engine Prometheus metrics on port `8080/metrics` and forwards them to Cloud Monitoring (`monitoring.googleapis.com:443`).
   - Log pipeline enriches application logs with trace IDs, span IDs, and Kubernetes pod labels, streaming them to Cloud Logging.
3. **Cloud Trace Waterfall**: Every transaction execution (Saga execution, Outbox commit, compensating action) can be visualized as an end-to-end distributed flamegraph in Google Cloud Console.

---

## 5. Comprehensive Command Runbook

### A. One-Click Cloud Bootstrap & Deployment
The repository includes an end-to-end automated deployment script [scripts/run/deploy_cloud.sh](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/scripts/run/deploy_cloud.sh):

```bash
# Execute one-click bootstrap: creates project, enables 14 APIs, provisions GCS bucket, applies Terraform, and deploys K8s workloads
./scripts/run/deploy_cloud.sh <PROJECT_ID> dev us-central1 [BILLING_ACCOUNT_ID]
```

---

### B. Manual Step-by-Step GCP CLI Commands

#### 1. Project Setup & Authentication
```bash
# Login to Google Cloud
gcloud auth login

# Set application default credentials for Terraform
gcloud auth application-default login

# Create or select project
export PROJECT_ID="my-dist-tx-project"
gcloud config set project "${PROJECT_ID}"

# Link billing account
gcloud billing projects link "${PROJECT_ID}" --billing-account="012345-6789AB-CDEF01"
```

#### 2. Enable All Required Google Cloud APIs
```bash
gcloud services enable \
    compute.googleapis.com \
    container.googleapis.com \
    sqladmin.googleapis.com \
    redis.googleapis.com \
    servicenetworking.googleapis.com \
    pubsub.googleapis.com \
    iam.googleapis.com \
    cloudresourcemanager.googleapis.com \
    secretmanager.googleapis.com \
    cloudkms.googleapis.com \
    storage.googleapis.com \
    artifactregistry.googleapis.com \
    cloudtrace.googleapis.com \
    logging.googleapis.com \
    monitoring.googleapis.com
```

#### 3. Create Terraform Remote State Storage Bucket
```bash
export REGION="us-central1"
export TF_STATE_BUCKET="${PROJECT_ID}-dist-tx-tfstate"

# Create bucket with uniform bucket level access
gcloud storage buckets create "gs://${TF_STATE_BUCKET}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access

# Enable object versioning to protect against accidental state corruption
gcloud storage buckets update "gs://${TF_STATE_BUCKET}" --versioning
```

#### 4. Configure Workload Identity
```bash
# Create dedicated Google Service Account for the engine
gcloud iam service-accounts create dist-tx-workload \
    --display-name="Distributed TX Engine Workload Identity GSA"

# Grant Cloud SQL Client, Pub/Sub, and Trace permissions
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/cloudsql.client"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/cloudtrace.agent"

# Bind KSA to GSA via Workload Identity User role
gcloud iam service-accounts add-iam-policy-binding \
    "dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/iam.workloadIdentityUser" \
    --member="serviceAccount:${PROJECT_ID}.svc.id.goog[dist-tx-dev/dist-tx-engine-sa]"
```

---

### C. Terraform Verification, Dry-Run & Provisioning Commands

#### 1. Offline Verification & Dry-Run (No Cloud Connection Needed)
```bash
cd deploy/terraform

# Check HCL formatting style
terraform fmt -check

# Initialize with remote backend disabled for offline syntax & provider schema validation
terraform init -backend=false

# Validate all resource arguments and references
terraform validate

# Run offline dry-run speculative plan with local state
terraform plan -refresh=false -var="project_id=${PROJECT_ID}" -var="environment=dev" -var="region=${REGION}"
```

#### 2. Live Cloud Execution Plan & Apply
```bash
cd deploy/terraform

# Initialize with real GCS remote backend
terraform init

# Generate binary execution plan
terraform plan \
    -var="project_id=${PROJECT_ID}" \
    -var="environment=dev" \
    -var="region=${REGION}" \
    -out=tfplan

# Review the 12 planned resources, then apply
terraform apply tfplan

# Inspect Terraform outputs
terraform output
```

#### 3. Connect Kubectl to Live GKE Cluster
```bash
# Fetch GKE cluster credentials into local ~/.kube/config
gcloud container clusters get-credentials dev-dist-tx-gke \
    --region "${REGION}" \
    --project "${PROJECT_ID}"

# Verify cluster nodes
kubectl get nodes -o wide

# Verify workloads
kubectl get pods,svc,hpa -A
```

# ADR-0006: Google Cloud Platform Infrastructure Provisioning via Terraform

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Core Architecture Guild |
| Related ADRs | [ADR-0004 (Observability Fabric)](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tech-stack/ADR-0004-lgtm-opentelemetry-observability.md), [ADR-0005 (Kubernetes Deployment)](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/kubernetes/adr.md) |
| Related System | [Terraform GCP Deployment](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/deploy/terraform) |

---

## 1. Context & Architectural Rationale ("Why Every Component Exists")

Deploying a mission-critical Distributed Transaction Engine onto Google Cloud Platform requires an immutable, declarative Infrastructure-as-Code (IaC) foundation. Unlike simple demo applications, a financial/transactional ledger cannot afford security oversights, unmetered network hops, or manual infrastructure drift.

Every single infrastructure component chosen in [deploy/terraform/main.tf](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/deploy/terraform/main.tf) is backed by an explicit architectural justification:

### A. Why a Custom VPC (`auto_create_subnetworks = false`)?
* **The Failure Mode of Default VPCs**: Google Cloud's default VPC automatically provisions subnets in every single worldwide region with default overlapping IP ranges and opens insecure default firewall rules (e.g. `default-allow-ssh` to `0.0.0.0/0`).
* **Our Decision**: A custom VPC grants complete control over CIDR block allocation, preventing IP collisions and ensuring that zero firewall ports are opened unless explicitly declared.

### B. Why Private Services Access Peering (`10.0.0.0/16`) for Cloud SQL & Redis?
* **The Vulnerability**: Connecting to Cloud SQL or Redis across the public internet—even with IP allowlists—exposes database ports to public DNS scanners, brute-force attempts, and distributed denial-of-service (DDoS).
* **Our Decision**: Peering reserves an RFC 1918 block (`10.0.0.0/16`) where Google provisions Cloud SQL and Memorystore Redis instances inside internal Google tenant VPCs. All database queries travel over private Google SDN fiber with zero internet routing and sub-millisecond round-trip latency.

### C. Why Cloud Router + Cloud NAT (`AUTO_ONLY`) for Private Nodes?
* **The Dilemma**: GKE worker nodes are provisioned with `enable_private_nodes = true` (they have zero external public IPs). However, the nodes must still pull container images from Docker Hub, query external payment gateway webhooks, and fetch Linux OS security patches.
* **Our Decision**: Cloud NAT provides stateful outbound-only egress. External internet hosts cannot initiate any unsolicited inbound connection to the nodes, eliminating the attack surface while preserving outbound access.

### D. Why Standalone Network Endpoint Groups (NEGs) instead of NodePort?
* **The Legacy NodePort Flaw**: In standard Kubernetes ingress, an external load balancer hits a worker node VM. The node's `kube-proxy` must rewrite packets via `iptables` or IPVS, hopping across the internal network to a pod running on a *different* node. This incurs a **2-hop penalty**, obfuscates the original client IP via SNAT, and causes uneven traffic distribution.
* **Our Decision**: With Container-Native Load Balancing via NEGs (`cloud.google.com/neg`), the Google Cloud HTTP(S) Load Balancer routes traffic directly to the individual Pod IP address (`10.20.x.x`). This yields true 1-hop latency, even pod traffic distribution, and preserves client source IPs.

### E. Why a Zero-Public-IP Compute Engine Bastion with Identity-Aware Proxy (IAP)?
* **The Operational Problem**: How do database administrators or CI/CD runners apply schema migrations (`psql`) or inspect Redis keys when both datastores reside inside a strictly private network with zero public IPs?
* **The Insecure Approach**: Assigning a public IP to a jumpbox VM with an SSH key is vulnerable to zero-day OpenSSH exploits and internet port scans.
* **Our Decision**: The bastion host (`e2-micro`) has no external IP address. Access is gated by Google Cloud Identity-Aware Proxy (IAP). Traffic is encrypted and tunneled through Google's edge proxies, requiring both IAM authentication (`roles/iap.tunnelResourceAccessor`) and multi-factor authentication (MFA).

### F. Why Workload Identity Federation (Eliminating JSON Service Account Keys)?
* **The Critical Risk of Static JSON Keys**: Long-lived service account keys stored in Kubernetes secrets or environment variables represent the #1 cause of cloud data breaches when accidentally leaked, checked into Git, or exfiltrated via SSRF attacks.
* **Our Decision**: Workload Identity binds Kubernetes Service Accounts (KSA) directly to Google Service Accounts (GSA). The GKE metadata server mints short-lived (1-hour TTL) OAuth2 access tokens on the fly without any static keys residing on disk.

---

## 2. Enabled Google Cloud APIs & IAM Permission Matrix

### A. Mandatory Google Cloud APIs
The infrastructure requires 15 core APIs enabled prior to running Terraform. The bootstrap script `scripts/run/deploy_cloud.sh` activates these APIs idempotently:

| API Identifier | Service Name | Why It Is Mandatory & Architectural Impact |
|---|---|---|
| `compute.googleapis.com` | Compute Engine API | Provisions the Custom VPC, Subnetworks, Cloud Router, Cloud NAT, Firewall rules, and the IAP Bastion VM. |
| `container.googleapis.com` | Kubernetes Engine (GKE) API | Manages the GKE control plane, autoscaling node pools, Network Endpoint Groups, and Workload Identity. |
| `sqladmin.googleapis.com` | Cloud SQL Admin API | Provisions and operates private Cloud SQL PostgreSQL 17 instances and manages automated backups. |
| `redis.googleapis.com` | Google Cloud Memorystore API | Provisions low-latency Redis 7.2 in-memory instances for distributed lock fencing. |
| `servicenetworking.googleapis.com` | Service Networking API | Establishes VPC Network Peering for Private Services Access (Cloud SQL & Redis). |
| `pubsub.googleapis.com` | Cloud Pub/Sub API | Provisions partitioned event topics and Dead Letter Queues (DLQ) for asynchronous saga events. |
| `iam.googleapis.com` | Identity and Access Management API | Manages GSAs, IAM role bindings, and Workload Identity token minting. |
| `cloudresourcemanager.googleapis.com` | Cloud Resource Manager API | Manages project-level IAM policies, billing associations, and organizational metadata. |
| `secretmanager.googleapis.com` | Secret Manager API | Stores database passwords and encryption secrets retrieved securely at application boot. |
| `cloudkms.googleapis.com` | Cloud Key Management Service (KMS) | Manages customer-managed encryption keys (CMEK) for disks and database volumes. |
| `storage.googleapis.com` | Cloud Storage API | Hosts remote Terraform state (`gs://<project>-dist-tx-tfstate`) with object versioning. |
| `artifactregistry.googleapis.com` | Artifact Registry API | Private OCI container registry storing the distributed transaction engine container images. |
| `cloudtrace.googleapis.com` | Cloud Trace API | Collects distributed trace spans forwarded by the cluster OpenTelemetry Collector. |
| `logging.googleapis.com` | Cloud Logging API | Ingests high-throughput structured JSON logs for audit trails and crash diagnostics. |
| `monitoring.googleapis.com` | Cloud Monitoring API | Scrapes metrics from GKE node cAdvisor and the OpenTelemetry Collector. |

---

### B. IAM Roles & Workload Identity Permissions

```
[KSA: dist-tx-engine-sa] ──(Workload Identity User)──▶ [GSA: dist-tx-workload]
                                                              │
                     ┌────────────────────────────────────────┼────────────────────────────────────────┐
                     ▼                                        ▼                                        ▼
             [Cloud SQL Client]                        [Pub/Sub Publisher]                   [Cloud Trace Agent]
        (roles/cloudsql.client)                     (roles/pubsub.publisher)              (roles/cloudtrace.agent)
```

| IAM Role | Granted To | Why This Role Is Required |
|---|---|---|
| `roles/container.nodeServiceAccount` | GKE Node GSA | Minimum privilege for GKE worker nodes to register with the master, pull images, and stream system logs. |
| `roles/iam.workloadIdentityUser` | GSA `dist-tx-workload` | Authorizes the Kubernetes Service Account (`dist-tx-engine-sa`) to impersonate the GSA and receive STS OAuth2 tokens. |
| `roles/cloudsql.client` | GSA `dist-tx-workload` | Authorizes the in-pod Cloud SQL Auth Proxy sidecar to establish mutual-TLS database sessions with PostgreSQL. |
| `roles/pubsub.publisher` | GSA `dist-tx-workload` | Allows the Transactional Outbox relay to publish transaction events to Google Cloud Pub/Sub. |
| `roles/pubsub.subscriber` | GSA `dist-tx-workload` | Allows the Saga execution engine to consume incoming compensation and trigger events. |
| `roles/cloudtrace.agent` | OTel Collector GSA | Allows OpenTelemetry Collector pods to push transaction trace spans to Google Cloud Trace. |
| `roles/monitoring.metricWriter` | OTel Collector GSA | Allows OpenTelemetry Collector pods to export custom engine metrics to Cloud Monitoring. |
| `roles/logging.logWriter` | OTel Collector GSA | Allows OpenTelemetry Collector pods to stream logs directly to Cloud Logging. |
| `roles/iap.tunnelResourceAccessor` | DevOps Engineers | Authorizes engineers to open an encrypted SSH tunnel through Cloud IAP to the bastion host. |

---

## 3. Network Routing, Cloud NAT & Firewall Hierarchy

### A. Routing & CIDR Allocation Scheme

```
VPC: ${environment}-dist-tx-vpc (auto_create_subnetworks = false)
 │
 ├── Subnetwork: ${environment}-dist-tx-subnet
 │    ├── Primary IPv4 Range:  10.10.0.0/20   (GKE Node VM Internal IPs: 4,096 IPs)
 │    ├── Secondary Range 1:   10.20.0.0/16   (GKE Pod Overlay IPs: 65,536 IPs)
 │    └── Secondary Range 2:   10.30.0.0/20   (GKE Service ClusterIPs: 4,096 IPs)
 │
 ├── Cloud Router & Cloud NAT Gateway (${environment}-dist-tx-nat)
 │    └── Translates egress RFC 1918 traffic from 10.10.0.0/20 & 10.20.0.0/16 to regional public IPs
 │
 └── Private Services Access Peering (servicenetworking.googleapis.com)
      └── Reserved Range:      10.0.0.0/16    (Private Service Peering: 65,536 IPs)
           ├── Cloud SQL PostgreSQL 17:       Assigned IP inside 10.0.0.0/16
           └── Memorystore Redis 7.2:         Assigned IP inside 10.0.0.0/16
```

### B. Firewall Security Rules Hierarchy

Custom VPCs drop all incoming traffic by default. The following rules enforce strict least-privilege traffic flow:

| Firewall Rule Name | Priority | Direction | Source Ranges / Tags | Allowed Protocols & Ports | Target / Purpose |
|---|---|---|---|---|---|
| `${env}-allow-internal-traffic` | 1000 | INGRESS | `10.10.0.0/20`, `10.20.0.0/16`, `10.30.0.0/20`, `10.0.0.0/16` | TCP: 0–65535, UDP: 0–65535, ICMP | Allows intra-cluster pod-to-pod, node-to-node, and datastore traffic. |
| `${env}-allow-gcp-health-checks` | 1000 | INGRESS | `130.211.0.0/22`, `35.191.0.0/16` | TCP: 8080, 443, 80, 13133 | Allows Google Cloud Load Balancer health checkers to probe GKE endpoints. |
| `${env}-allow-iap-ssh` | 1000 | INGRESS | `35.235.240.0/20` (Google IAP) | TCP: 22 | Allows authorized SSH traffic via Cloud IAP into the Bastion Host (`bastion-host`). |
| `${env}-allow-gke-master-to-nodes` | 1000 | INGRESS | `172.16.0.0/28` (GKE Master) | TCP: 443, 10250, 8443, 9443 | Allows GKE master control plane to execute admission webhooks and scrape Kubelet metrics. |
| Default Deny Ingress | 65535 | INGRESS | `0.0.0.0/0` | ALL | Blocks all unsolicited external inbound traffic. |

---

## 4. Failure-First Root Cause Analysis & Disaster Recovery Matrix

In cloud infrastructure, misconfigurations do not cause graceful warnings—they cause catastrophic deployment deadlocks, silent packet dropping, and permanent state corruption.

| Failure Mode | Root Cause | Real-World Impact | Technical Resolution & Runbook Command |
|---|---|---|---|
| **Terraform GCS State Lock Deadlock (`Error acquiring the state lock`)** | A CI/CD runner crashed or was killed (SIGKILL / timeout) while executing `terraform apply`, leaving the lock object in `gs://${TF_STATE_BUCKET}` active. | All subsequent team PRs and deployment pipelines are completely blocked with `Lock Info: ID: 169829381`. | Run: `terraform force-unlock <LOCK_ID>`. Verify in CI/CD pipeline settings that pipeline timeouts send graceful SIGINT rather than SIGKILL. |
| **GKE Private Cluster Admission Webhook Freeze** | The GKE master control plane runs in `172.16.0.0/28`. When pods use validating/mutating admission webhooks (e.g. Istio, Calico, OTel Operator) on ports 8443/9443, the custom VPC firewall blocks the master from reaching the worker node. | All `kubectl apply` commands hang indefinitely and time out with `Internal error occurred: failed calling webhook`. | Provisioned firewall rule `${env}-allow-gke-master-to-nodes` allowing TCP `443, 10250, 8443, 9443` from `172.16.0.0/28` to target tag `gke-node`. |
| **Cloud NAT Port Allocation Exhaustion (`IP_OUT_OF_RESOURCES`)** | During a 15,000+ RPS surge, engine pods initiate hundreds of outbound HTTPS requests to external SaaS APIs. With default NAT port allocation (64 ports per VM), source ports are instantly exhausted. | Outbound payment API and webhook calls fail intermittently with `Connection timed out` or `Cannot assign requested address`. | Configured `enable_endpoint_independent_mapping = true` and increased `min_ports_per_vm = 64` with auto-scaling NAT IPs to allocate dynamic IP pools automatically. |
| **Cloud SQL Deletion Protection vs Terraform Destroy Failure** | `google_sql_database_instance.postgres` sets `deletion_protection = true`. Running `terraform destroy` halts with `cannot delete instance with deletion_protection enabled`. | Automated ephemeral test environments fail to tear down, incurring continuous idle billing. | Run `terraform apply -var="deletion_protection=false"` to explicitly toggle off the protection before executing `terraform destroy`. |
| **Private Services Peering CIDR Exhaustion** | When `private-ip-alloc` is sized too small (e.g. `/24`), adding a Cloud SQL read replica or expanding Memorystore Redis fails with `Error 400: IP pool exhausted for peering`. | In-flight scaling operations fail during traffic surges, leading to database CPU saturation. | Reserved `/16` (65,536 IPs) via `prefix_length = 16` in `google_compute_global_address.private_ip_address`, guaranteeing sufficient headroom for dozens of replicas. |
| **GKE Master Private Endpoint Isolation Lockout** | Setting `enable_private_endpoint = true` without defining `master_authorized_networks_config` cuts off all external access to the Kubernetes API server. | Developers and GitHub Actions runners cannot run `kubectl` commands (`Unable to connect to the server: dial tcp: i/o timeout`). | Kept `enable_private_endpoint = false` with public master endpoint and private worker nodes (`enable_private_nodes = true`), or route `kubectl` through the IAP bastion host. |

---

## 5. Beginner's Comprehensive CLI Command Runbook (From Scratch)

This section is designed for students and engineers learning Google Cloud and Terraform from the ground up. Every command is explained with its purpose, flags, and expected outputs.

---

### Step 1: Initial Setup, Authentication & Project Configuration

```bash
# 1.1 Verify Google Cloud CLI installation and version
gcloud version

# 1.2 Authenticate your personal identity with Google Cloud
# This opens a web browser to log in with your Google account.
gcloud auth login

# 1.3 Authenticate Application Default Credentials (ADC)
# This generates an authentication token (~/.config/gcloud/application_default_credentials.json)
# that Terraform, Python SDK, and Go client libraries use to communicate with Google APIs.
gcloud auth application-default login

# 1.4 List all projects you have access to
gcloud projects list

# 1.5 Define your target project ID, region, and environment variables
export PROJECT_ID="my-dist-tx-project"
export REGION="us-central1"
export ENVIRONMENT="dev"

# 1.6 Set the active project for all subsequent gcloud commands
gcloud config set project "${PROJECT_ID}"

# 1.7 Confirm active configuration
gcloud config list
```

---

### Step 2: Enable All Required Google Cloud APIs

```bash
# In Google Cloud, all API services are disabled by default to prevent unexpected billing.
# We must explicitly enable the 15 services required by our Terraform configuration:
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

# Verify that all 15 APIs are currently enabled
gcloud services list --enabled --filter="name:(compute OR container OR sqladmin OR redis OR pubsub)"
```

---

### Step 3: Create the Terraform Remote State GCS Storage Bucket

```bash
# Terraform stores the mapping between your HCL code and real cloud resources in a "state file" (terraform.tfstate).
# Storing this file locally is dangerous (risk of data loss, lack of team collaboration, lack of state locking).
# We store it inside a Google Cloud Storage (GCS) bucket.

export TF_STATE_BUCKET="${PROJECT_ID}-dist-tx-tfstate"

# 3.1 Create the GCS bucket with uniform bucket-level access (disables legacy ACLs for security)
gcloud storage buckets create "gs://${TF_STATE_BUCKET}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access

# 3.2 Enable Object Versioning
# If a Terraform operation ever corrupts the state, versioning allows you to roll back to any previous state version!
gcloud storage buckets update "gs://${TF_STATE_BUCKET}" --versioning

# 3.3 Verify bucket creation and properties
gcloud storage buckets describe "gs://${TF_STATE_BUCKET}"
```

---

### Step 4: Configure Workload Identity Federation (Zero-Key Security)

```bash
# 4.1 Create the Google Service Account (GSA)
gcloud iam service-accounts create dist-tx-workload \
    --description="Workload Identity service account for Distributed TX Engine pods" \
    --display-name="Distributed TX Engine GSA"

# 4.2 Grant Cloud SQL Client role to allow mutual-TLS database connectivity
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/cloudsql.client"

# 4.3 Grant Pub/Sub Publisher role to allow Outbox event publishing
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"

# 4.4 Grant Pub/Sub Subscriber role to allow Saga event consumption
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/pubsub.subscriber"

# 4.5 Grant Cloud Trace Agent role to allow telemetry span streaming
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/cloudtrace.agent"

# 4.6 Bind the Kubernetes Service Account (KSA) to the GSA
# This tells Google Cloud: "Allow pods running under KSA 'dist-tx-engine-sa' in namespace 'dist-tx-dev' to act as this GSA"
gcloud iam service-accounts add-iam-policy-binding \
    "dist-tx-workload@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/iam.workloadIdentityUser" \
    --member="serviceAccount:${PROJECT_ID}.svc.id.goog[dist-tx-dev/dist-tx-engine-sa]"
```

---

### Step 5: Terraform Deep-Dive Operations & Inspection

Navigate to the Terraform directory:
```bash
cd deploy/terraform
```

#### 5.1 Format Code According to Canonical Standards
```bash
# Re-aligns indentation, equals signs, and bracket formatting
terraform fmt

# Run in CI mode: checks formatting without modifying files (exits with code 3 if unformatted)
terraform fmt -check -diff
```

#### 5.2 Initialize Terraform (Offline Validation Mode)
```bash
# Initializes provider plugins (google, kubernetes) without attempting to connect to the remote GCS backend.
# Perfect for offline syntax checking and pre-commit hooks!
terraform init -backend=false

# Validates syntax, attribute names, and variable types against the provider schemas
terraform validate
```

#### 5.3 Offline Dry-Run Speculative Plan
```bash
# Generates a complete dependency graph and plans resource creation WITHOUT contacting GCP state APIs.
# Expect output: "Plan: 19 to add, 0 to change, 0 to destroy."
terraform plan \
    -refresh=false \
    -var="project_id=${PROJECT_ID}" \
    -var="environment=dev" \
    -var="region=us-central1"
```

#### 5.4 Live Initialization with Remote GCS Backend
```bash
# Initializes Terraform and locks the state file in your Google Cloud Storage bucket
terraform init
```

#### 5.5 Generate and Save Binary Execution Plan
```bash
# Computes the delta between current GCP cloud state and your Terraform code, saving the execution plan to 'tfplan'.
# Saving to a file guarantees that 'terraform apply' executes the exact inspected plan without race conditions.
terraform plan \
    -var="project_id=${PROJECT_ID}" \
    -var="environment=dev" \
    -var="region=us-central1" \
    -out=tfplan
```

#### 5.6 Inspect the Binary Plan in Human-Readable Text
```bash
# Read the binary plan file to verify the 19 planned resources before executing
terraform show tfplan
```

#### 5.7 Apply the Infrastructure Plan
```bash
# Executes the plan against Google Cloud APIs to provision the VPC, Subnet, NAT, GKE, Cloud SQL, Redis, and Bastion!
terraform apply tfplan

# Inspect Terraform outputs after completion
terraform output
```

#### 5.8 Inspect and Query Terraform State
```bash
# List all 19 tracked cloud resources in state
terraform state list

# Show detailed state attributes for a specific resource (e.g. Cloud SQL instance)
terraform state show google_sql_database_instance.postgres

# View the full state file in JSON format
terraform show -json | jq '.values.root_module.resources[].address'
```

---

### Step 6: Connect Kubectl, Inspect GKE & Validate NEGs

```bash
# 6.1 Retrieve GKE cluster credentials into your local ~/.kube/config
gcloud container clusters get-credentials dev-dist-tx-gke \
    --region "${REGION}" \
    --project "${PROJECT_ID}"

# 6.2 Verify cluster connectivity and worker nodes
kubectl get nodes -o wide

# 6.3 Inspect GKE Cluster Autoscaler status
kubectl describe configmap cluster-autoscaler-status -n kube-system

# 6.4 List standalone Network Endpoint Groups (NEGs) provisioned in the region
gcloud compute network-endpoint-groups list --filter="zone:${REGION}"

# 6.5 Inspect the individual Pod IP endpoints registered in a NEG
gcloud compute network-endpoint-groups list-network-endpoints <NEG_NAME> \
    --zone="${REGION}-a"
```

---

### Step 7: Accessing Private Cloud SQL & Redis via the IAP Bastion Host

Because PostgreSQL 17 and Redis 7.2 have no public IP addresses, you connect securely through the Compute Engine bastion host using Google Cloud Identity-Aware Proxy (IAP) TCP tunneling:

```bash
# 7.1 SSH into the private bastion host (zero public IP, zero open inbound firewall ports)
gcloud compute ssh dev-bastion-host \
    --zone="${REGION}-a" \
    --tunnel-through-iap

# 7.2 Forward the remote Cloud SQL PostgreSQL port (5432) to your local machine (5433)
# Leave this terminal running:
gcloud compute start-iap-tunnel dev-bastion-host 5432 \
    --local-host-port=localhost:5433 \
    --zone="${REGION}-a"

# 7.3 Connect using your local PostgreSQL client (psql) through the encrypted tunnel:
psql -h localhost -p 5433 -U postgres -d dist_tx_engine -c "SELECT version();"

# 7.4 Forward the remote Redis port (6379) to your local machine (6379)
gcloud compute start-iap-tunnel dev-bastion-host 6379 \
    --local-host-port=localhost:6379 \
    --zone="${REGION}-a"

# 7.5 Ping Redis through the secure IAP tunnel
redis-cli -h localhost -p 6379 ping
```

---

### Step 8: Destroying Infrastructure Cleanly (Tear-down)

```bash
# When testing is complete and you want to decommission all cloud resources to prevent ongoing billing:
terraform plan -destroy -out=destroy-plan
terraform apply destroy-plan

# Finally, delete the remote state bucket if completely removing the project:
# gcloud storage rm -r "gs://${TF_STATE_BUCKET}"
```

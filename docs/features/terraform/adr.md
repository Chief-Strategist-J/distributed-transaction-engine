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
3. **Egress Internet via Cloud NAT**: Private GKE nodes without external IPs require an egress gateway (Cloud Router + Cloud NAT) to pull public container images and query external SaaS APIs without exposing ingress vulnerabilities.
4. **Container-Native Ingress via NEGs**: Routing traffic through legacy NodePort proxies introduces an unnecessary extra network hop and uneven load distribution. Network Endpoint Groups (NEGs) allow Google Cloud Load Balancers to route directly to Pod IP addresses.
5. **Secure Administrative Bastion**: To execute database migrations or troubleshoot private datastores without exposing ports to the public internet, a Compute Engine bastion host accessible strictly via Google Cloud Identity-Aware Proxy (IAP) is mandated.

---

## 2. Enabled Google Cloud APIs & IAM Permission Matrix

### A. Mandatory Google Cloud APIs
The infrastructure requires 14 core APIs enabled prior to running Terraform. The bootstrap script `scripts/run/deploy_cloud.sh` activates these APIs idempotently:

| API Identifier | Service Name | Justification & Architectural Responsibility |
|---|---|---|
| `compute.googleapis.com` | Compute Engine API | Manages Custom VPC, Subnetworks, Cloud Router, Cloud NAT, Firewalls, and Bastion VM. |
| `container.googleapis.com` | Kubernetes Engine (GKE) API | Manages GKE control plane, autoscaling node pools, NEGs, and Workload Identity. |
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
| `roles/iap.tunnelResourceAccessor` | DevOps Engineers | Allows developers to tunnel SSH through Cloud IAP to the bastion host without public IP. |

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
| Default Deny Ingress | 65535 | INGRESS | `0.0.0.0/0` | ALL | Blocks all unsolicited external inbound traffic. |

---

## 4. Container-Native Load Balancing & Network Endpoint Groups (NEGs)

### Why Standalone NEGs Replace Legacy NodePort Routing
In standard Kubernetes `NodePort` ingress, external load balancers target worker node VM IPs. The node's `kube-proxy` then rewrites packets via `iptables` or IPVS to forward traffic to the actual pod running on a different node. This results in:
- **Two Network Hops**: Ingress -> Node VM -> Target Pod VM.
- **SNAT Source IP Masking**: Client IP is obscured unless `externalTrafficPolicy: Local` is used (which creates traffic imbalance).
- **Uneven Load Balancing**: Nodes with 1 pod receive equal traffic to nodes with 5 pods.

**The Solution: Container-Native Load Balancing via NEGs (`cloud.google.com/neg`)**:
1. GKE automatically registers individual pod IP addresses directly into a Google Cloud Network Endpoint Group (NEG).
2. The Google Cloud Application Load Balancer directly terminates connections and routes packets straight to the Pod IP (`10.20.x.x`).
3. Eliminates `kube-proxy` SNAT, achieves true 1-hop latency, and routes traffic evenly across all scaled pods.

---

## 5. Compute Engine Bastion Host (IAP-Only Access)

Because PostgreSQL 17 and Redis 7.2 have no public IP addresses and reject non-VPC traffic:
1. A lightweight `e2-micro` Compute Engine instance (`${environment}-bastion-host`) is provisioned inside `${environment}-dist-tx-subnet`.
2. The VM has **zero external IPv4 addresses** (`nat_ip` is omitted).
3. Engineers and CI/CD migration runners connect using Google Cloud Identity-Aware Proxy (IAP) TCP tunneling:
   ```bash
   gcloud compute start-iap-tunnel dev-bastion-host 5432 --local-host-port=localhost:5432 --zone=us-central1-a
   ```
4. Access is strictly audited and governed by GCP IAM role `roles/iap.tunnelResourceAccessor`.

---

## 6. Live Scaling & Resilience Architecture

### A. Dynamic Scaling Tiers
1. **GKE Cluster Autoscaler (`google_container_node_pool.primary_nodes`)**:
   - `min_node_count = 2` (steady state: supports 1,000–5,000 RPS).
   - `max_node_count = 10` (high surge: expands to 40 vCPUs, 160 GB RAM supporting 25,000+ RPS).
   - Node pool management enables `auto_repair = true` and `auto_upgrade = true` for zero-touch kernel security updates.
2. **Kubernetes Horizontal Pod Autoscaler (HPA)**:
   - Target metrics: Average CPU utilization > 75% or Memory utilization > 80%.
   - Scales engine pods from 2 replicas to 20 replicas.
3. **Cloud SQL Storage Autoscaling**:
   - `disk_autoresize = true` dynamically increases SSD storage in 5 GB increments when free space falls below 10%, preventing transaction lockups from outbox bloat.
4. **Cloud Pub/Sub Scalability**:
   - Partitioned message broker automatically scales throughput across Google global infrastructure without manual partition rebalancing.

---

## 7. Comprehensive Command Runbook

### A. One-Click Cloud Deployment
```bash
# Execute end-to-end bootstrap: APIs, GCS state, Terraform apply, and K8s workload rollout
./scripts/run/deploy_cloud.sh <PROJECT_ID> dev us-central1 [BILLING_ACCOUNT_ID]
```

### B. Offline Terraform Verification & Dry-Run
```bash
cd deploy/terraform

# 1. Format code according to canonical HCL standards
terraform fmt -check

# 2. Initialize without backend connection for offline schema validation
terraform init -backend=false

# 3. Validate syntax, resource types, and input parameters
terraform validate

# 4. Perform a 100% offline speculative plan (Expect: 19 to add)
terraform plan -refresh=false -var="project_id=mock-project" -var="environment=dev" -var="region=us-central1"
```

### C. Live Cloud Provisioning Commands
```bash
cd deploy/terraform

# 1. Initialize with real GCS remote state bucket
terraform init

# 2. Generate and output binary plan
terraform plan \
    -var="project_id=${PROJECT_ID}" \
    -var="environment=dev" \
    -var="region=us-central1" \
    -out=tfplan

# 3. Apply the 19 planned resources
terraform apply tfplan

# 4. View provisioned infrastructure outputs
terraform output
```

### D. Secure Bastion & Database Access via Cloud IAP
```bash
# SSH into private bastion host with zero public IP
gcloud compute ssh dev-bastion-host --zone=us-central1-a --tunnel-through-iap

# Port-forward Cloud SQL PostgreSQL port to local workstation
gcloud compute start-iap-tunnel dev-bastion-host 5432 \
    --local-host-port=localhost:5433 \
    --zone=us-central1-a

# Connect via local psql through the secure encrypted IAP tunnel
psql -h localhost -p 5433 -U postgres -d dist_tx_engine
```

### E. GKE Cluster & NEG Inspection Commands
```bash
# Retrieve cluster credentials
gcloud container clusters get-credentials dev-dist-tx-gke --region us-central1 --project "${PROJECT_ID}"

# Inspect GKE nodes and autoscaler status
kubectl get nodes -o wide
kubectl describe configmap cluster-autoscaler-status -n kube-system

# Inspect standalone Network Endpoint Groups (NEGs)
gcloud compute network-endpoint-groups list --filter="zone:us-central1"

# View Pod endpoints bound to the NEG
gcloud compute network-endpoint-groups list-network-endpoints <NEG_NAME> --zone=us-central1-a
```

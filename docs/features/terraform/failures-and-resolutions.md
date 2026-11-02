# Google Cloud & Terraform Failure-First Root Cause Analysis & Deep-Dive Runbook

This document details critical real-world failure modes encountered in Google Cloud Platform infrastructure provisioning via Terraform for the Distributed Transaction Engine. For each failure mode, this guide covers the real-user scenario, root cause, exact recovery runbook, and an exhaustive, side-by-side Before-vs-After architecture diagram illustrating packet paths, state transitions, and component interactions.

---

## 1. Terraform Remote State Lock Deadlock (`Error acquiring the state lock`)

### A. Real-User Scenario
During a CI/CD automated deployment, an agent runner crashes abruptly (OOM or runner timeout) during a `terraform apply`. When any team member tries to run a new deployment or PR verification, Terraform immediately terminates with:
```
Error: Error acquiring the state lock
Lock Info:
  ID:        1729381920381
  Path:      gs://project-dist-tx-tfstate/terraform/state/default.tflock
  Who:       runner-482@ci-worker-node-1
  Created:   2026-11-02 08:30:15 UTC
```
All production deployments and hotfixes are completely paralyzed.

### B. Root Cause
Terraform places a lock object in Google Cloud Storage when any write operation begins. Because the process was forcefully killed via SIGKILL without a clean exit handler, the lock file remained orphaned in the GCS bucket, permanently rejecting subsequent operations.

### C. How Real Engineers Solve It
```bash
# 1. Inspect the exact lock ID from the Terraform error output
export LOCK_ID="1729381920381"

# 2. Force-unlock the remote GCS state
terraform force-unlock "${LOCK_ID}"

# 3. If force-unlock fails due to API permissions, remove the lock object directly via gcloud:
gcloud storage rm "gs://${PROJECT_ID}-dist-tx-tfstate/terraform/state/default.tflock"
```

### D. Side-by-Side: Before vs. After Failure Resolution

```mermaid
graph LR
    subgraph "BEFORE: Orphaned Remote State Lock (Complete Pipeline Deadlock)"
        direction TB
        CiRunner["CI/CD Runner (Killed by OOM / SIGKILL mid-apply)"]
        GcsBucket[("GCS Bucket: gs://project-dist-tx-tfstate")]
        LockObject["Orphaned Lock: default.tflock (Active)"]
        DevPr["Developer PR Validation Run"]
        SreHotfix["Hotfix Deployment Workflow"]
        LockError["Terraform Exits with Code 1: Error acquiring state lock!"]

        CiRunner -.->|Crash without Exit Handler| LockObject
        LockObject --- GcsBucket
        DevPr -->|Query Remote State| GcsBucket
        SreHotfix -->|Query Remote State| GcsBucket
        GcsBucket --> LockError
    end

    subgraph "AFTER: Atomic State Unlock & Pipeline Resumption"
        direction TB
        SreAction["DevOps / SRE Engineer"]
        ForceUnlockCmd["terraform force-unlock 1729381920381"]
        GcsBucket2[("GCS Bucket: gs://project-dist-tx-tfstate")]
        LockDeleted["Lock Object Cleanly Removed from GCS"]
        PendingDeploy["CI/CD Automation Runner"]
        TerraformPlan["terraform plan / apply Acquires New Transient Lock"]
        RolloutSuccess["Infrastructure Changes Successfully Applied"]

        SreAction --> ForceUnlockCmd --> LockDeleted
        LockDeleted ==>|State Unlocked| GcsBucket2
        PendingDeploy --> GcsBucket2 --> TerraformPlan --> RolloutSuccess
    end
```

---

## 2. GKE Private Cluster Admission Webhook Freeze

### A. Real-User Scenario
Engineers enable private cluster mode (`enable_private_nodes = true`). When deploying workloads with admission webhooks (such as Calico NetworkPolicy, cert-manager, or OTel Operator), running `kubectl apply` hangs indefinitely and crashes after 30 seconds with:
```
Error from server (InternalError): Internal error occurred: 
failed calling webhook "validate.calico.org": failed to call webhook: 
Post "https://calico-webhook.calico-system.svc:443/validate": dial tcp 10.20.1.15:443: i/o timeout
```

### B. Root Cause
In private GKE clusters, the control plane master resides in an isolated Google-managed VPC with CIDR `172.16.0.0/28`. Because the custom VPC has zero default inbound firewall rules, worker node VMs drop incoming TCP connections from the master to ports 443, 10250, 8443, and 9443. The Kubernetes API server cannot contact the admission webhook running inside the worker node pod.

### C. How Real Engineers Solve It
1. **Immediate Firewall Addition via gcloud**:
   ```bash
   gcloud compute firewall-rules create dev-allow-gke-master-to-nodes \
       --network="${ENVIRONMENT}-dist-tx-vpc" \
       --action=ALLOW \
       --direction=INGRESS \
       --source-ranges="172.16.0.0/28" \
       --rules="tcp:443,tcp:10250,tcp:8443,tcp:9443" \
       --target-tags="gke-node"
   ```
2. **Permanent Terraform Declaration**: Added resource `google_compute_firewall.allow_gke_master_to_nodes` to [deploy/terraform/main.tf](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/deploy/terraform/main.tf).

### D. Side-by-Side: Before vs. After Failure Resolution

```mermaid
graph LR
    subgraph "BEFORE: Control Plane Egress Blocked (Admission Webhook Timeout)"
        direction TB
        KubeApply["Developer: kubectl apply -f deployment.yaml"]
        GkeApiServer["GKE Master API Server (IP: 172.16.0.2)"]
        VpcFirewall["VPC Firewall (Default Deny Ingress)"]
        PacketDrop["TCP SYN to :8443 DROPPED by Node Firewall"]
        WebhookPod["Calico / OTel Admission Webhook Pod (10.20.1.15:8443)"]
        HangCrash["API Server Hangs 30s -> InternalError: dial tcp i/o timeout"]

        KubeApply --> GkeApiServer -->|Invoke Validating Webhook| VpcFirewall
        VpcFirewall --> PacketDrop
        PacketDrop -.->|Never Delivered| WebhookPod
        GkeApiServer --> HangCrash
    end

    subgraph "AFTER: Master-to-Node Firewall Rule Active (Instant Admission)"
        direction TB
        KubeApply2["Developer: kubectl apply -f deployment.yaml"]
        GkeApiServer2["GKE Master API Server (IP: 172.16.0.2)"]
        AllowedFw["Firewall: allow-gke-master-to-nodes (Ports 443, 8443, 9443)"]
        ActiveWebhookPod["Admission Webhook Pod (10.20.1.15:8443)"]
        ValidationAck["Admission Review: Allowed = true (Response in 2ms)"]
        PodCreated["Deployment Object Committed to etcd & Pods Scheduled"]

        KubeApply2 --> GkeApiServer2 ==>|Permitted Traffic| AllowedFw
        AllowedFw --> ActiveWebhookPod --> ValidationAck --> PodCreated
    end
```

---

## 3. Cloud NAT Port Allocation Exhaustion (`IP_OUT_OF_RESOURCES`)

### A. Real-User Scenario
During high-traffic transaction surges, engine pods make external outbound HTTPS calls to third-party banking APIs, webhooks, and OIDC auth providers. Intermittently, outbound requests fail with:
```
dial tcp: lookup api.stripe.com: i/o timeout
connect: cannot assign requested address
```
Cloud NAT metrics show dropped flows with status `DROPPED_OUT_OF_RESOURCES`.

### B. Root Cause
Each worker node VM is allocated a fixed number of source ports (default 64) for outbound NAT mapping. When thousands of concurrent outbound connections are opened by engine pods, the node VM runs out of available ephemeral source port tuples, dropping outbound TCP packets.

### C. How Real Engineers Solve It
1. **Immediate Cloud NAT Configuration via gcloud**:
   ```bash
   gcloud compute routers nats update dev-dist-tx-nat \
       --router=dev-dist-tx-router \
       --region=us-central1 \
       --min-ports-per-vm=128 \
       --enable-endpoint-independent-mapping
   ```
2. **Permanent Terraform Fix**: Configured `enable_endpoint_independent_mapping = true` and dynamic IP allocation in `google_compute_router_nat.nat`.

### D. Side-by-Side: Before vs. After Failure Resolution

```mermaid
graph LR
    subgraph "BEFORE: Static 64 NAT Ports Exhausted (Intermittent Dropped Flows)"
        direction TB
        EngineRps["Engine Pods (High Outbound Burst: 5,000 Outbound Conns)"]
        NodeInterface["Node VM Network Namespace (10.10.0.5)"]
        StaticNAT["Cloud NAT (Fixed 64 Ports Per VM: 104.198.1.1:1024..1087)"]
        PortExhaustion{"Ports Available?"}
        DroppedFlow["Drop: IP_OUT_OF_RESOURCES (Cannot assign requested address)"]
        SaaSFailure["External Payment API Calls Fail (Transactions Aborted)"]

        EngineRps --> NodeInterface --> StaticNAT --> PortExhaustion
        PortExhaustion -->|No: Exhausted| DroppedFlow --> SaaSFailure
    end

    subgraph "AFTER: Dynamic NAT Ports & Endpoint Independent Mapping"
        direction TB
        EngineRps2["Engine Pods (High Outbound Burst: 5,000 Outbound Conns)"]
        NodeInterface2["Node VM Network Namespace (10.10.0.5)"]
        DynamicNAT["Cloud NAT (Auto-Allocating IP Pool + Endpoint Independent)"]
        MultiIPs["Regional Public IPs (Pool: 104.198.1.1, 104.198.1.2, 104.198.1.3)"]
        CleanEgress["Seamless Outbound HTTPS Sessions Established"]
        PaymentSuccess["100% External Payment & Webhook Sagas Committed Cleanly"]

        EngineRps2 --> NodeInterface2 --> DynamicNAT ==>|Dynamic Port Spreading| MultiIPs
        MultiIPs --> CleanEgress --> PaymentSuccess
    end
```

---

## 4. Private Service Peering CIDR Exhaustion

### A. Real-User Scenario
As database usage grows, the operations team tries to create a Cloud SQL read replica or provision a new Memorystore Redis instance. The Terraform apply fails with:
```
Error: Error creating Instance: googleapi: Error 400: IP pool exhausted for peering.
The reserved peering range "dev-private-ip-alloc" has insufficient available addresses.
```

### B. Root Cause
The initial Terraform configuration allocated a small `/24` subnet (only 256 IPs). Cloud SQL requires a minimum `/24` per instance family, and Memorystore Redis requires additional dedicated sub-ranges. Sizing the initial peering reservation too small prevents expanding storage replicas.

### C. How Real Engineers Solve It
```bash
# 1. Inspect existing allocated peering range
gcloud compute addresses describe dev-private-ip-alloc --global

# 2. Reserve an expansive /16 range (65,536 IPs) in Terraform:
# resource "google_compute_global_address" "private_ip_address" {
#   prefix_length = 16
# }

# 3. Update the Service Networking connection peering range
gcloud services vpc-peerings update \
    --service=servicenetworking.googleapis.com \
    --ranges=dev-private-ip-alloc \
    --network=dev-dist-tx-vpc
```

### D. Side-by-Side: Before vs. After Failure Resolution

```mermaid
graph LR
    subgraph "BEFORE: Small /24 Peering Reservation (Scale-Out Blocked)"
        direction TB
        SmallRange["Reserved Peering CIDR: 10.0.0.0/24 (Total: 256 IPs)"]
        PrimaryPostgres["Primary Cloud SQL PostgreSQL (Consumes 10.0.0.0/24)"]
        RedisInstance["Memorystore Redis (Requires /28 Block)"]
        NewReplica["New Cloud SQL Read Replica (Requires additional /24)"]
        AllocCrash["GCP Service Networking: Error 400 IP Pool Exhausted!"]

        SmallRange === PrimaryPostgres
        SmallRange -.->|Insufficient Free Space| RedisInstance
        SmallRange -.->|Fails| NewReplica --> AllocCrash
    end

    subgraph "AFTER: Generous /16 Peering Reservation (Seamless Elastic Scaling)"
        direction TB
        LargeRange["Reserved Peering CIDR: 10.0.0.0/16 (Total: 65,536 IPs)"]
        CloudSqlBlock["Cloud SQL Range: 10.0.0.0/20 (Primary + 15 Read Replicas)"]
        RedisBlock["Memorystore Redis Range: 10.0.16.0/20 (High-Availability Clustered)"]
        FutureServices["Future Service Networking Attachments (AlloyDB, Vertex AI)"]
        ScaleSuccess["Infrastructure Scales Out Freely Without Network Reconstruction"]

        LargeRange ==> CloudSqlBlock & RedisBlock & FutureServices
        CloudSqlBlock & RedisBlock --> ScaleSuccess
    end
```

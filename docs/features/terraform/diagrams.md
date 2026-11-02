# Google Cloud Platform & Terraform Architecture Diagrams

This document contains detailed architectural diagrams illustrating the GCP network topology, API enablement & IAM authorization, live production execution flow, scaling mechanics, and cloud telemetry pipeline.

---

## 1. VPC Network Topology & Private Service Access Architecture

This diagram visualizes how Google Cloud VPC, Subnetworks, Secondary CIDRs, and Service Networking Peering interconnect GKE nodes, pods, Cloud SQL, and Memorystore Redis without any public IPs.

```mermaid
graph TB
    subgraph "External Clients & Ingress Tier"
        InternetUsers["External Microservices & Web Clients"]
        CloudArmor["Google Cloud Armor (WAF & DDoS Defense)"]
        ExternalLB["Global External HTTP(S) Load Balancer (Static IPv4/v6)"]
    end

    subgraph "Google Cloud VPC: ${environment}-dist-tx-vpc"
        subgraph "Customer Subnetwork: ${environment}-dist-tx-subnet"
            NodeSubnet["Primary Node CIDR: 10.10.0.0/20 (GKE Worker Node VMs)"]
            PodSecondary["Secondary Pod CIDR: 10.20.0.0/16 (VPC-Native Pods)"]
            SvcSecondary["Secondary Service CIDR: 10.30.0.0/20 (ClusterIPs)"]
        end

        subgraph "Private Services Peering Allocation: 10.0.0.0/16"
            GlobalAlloc["google_compute_global_address: private-ip-alloc"]
            VpcPeeringLink["Service Networking Peering Link (servicenetworking.googleapis.com)"]
        end
    end

    subgraph "Google-Managed Tenant VPC Network"
        CloudSQL["Cloud SQL PostgreSQL 17 (Private IP: 10.0.0.5)"]
        RedisInstance["Memorystore Redis 7.2 (Private IP: 10.0.0.10)"]
    end

    subgraph "Google Managed Control Plane"
        GkeMaster["GKE Control Plane Master (API Server :443)"]
    end

    InternetUsers --> CloudArmor --> ExternalLB
    ExternalLB -->|Forward to NodePort| NodeSubnet
    NodeSubnet -->|Routes traffic into| PodSecondary
    PodSecondary -->|Service Discovery| SvcSecondary

    GlobalAlloc --> VpcPeeringLink
    VpcPeeringLink <== Internal SDN Private Peering ==> CloudSQL & RedisInstance

    PodSecondary -->|TLS Encrypted SQL via Auth Proxy Sidecar| CloudSQL
    PodSecondary -->|Direct TCP Lock Fencing (:6379)| RedisInstance
    NodeSubnet -.->|Private Master Endpoint| GkeMaster
```

---

## 2. API Enablement, IAM Roles & Workload Identity Federation

This sequence diagram details how required Google Cloud APIs are activated and how an engine pod inside GKE authenticates to Google Cloud services without static service account keys.

```mermaid
sequenceDiagram
    autonumber
    participant CLI as DevOps Engineer / CI Runner
    participant ServiceUsage as Google Service Usage API
    participant IAM as Google Cloud IAM Service
    participant GKE as GKE Kubelet & Pod (KSA)
    participant STS as Google Security Token Service (STS)
    participant CloudSQL as Cloud SQL Admin API
    participant PubSub as Google Cloud Pub/Sub API
    participant Trace as Google Cloud Trace API

    Note over CLI,ServiceUsage: Phase 1: API Enablement (One-Time Bootstrap)
    CLI->>ServiceUsage: gcloud services enable (container, compute, sqladmin, redis, pubsub, iam, trace)
    ServiceUsage-->>CLI: HTTP 200 OK (APIs Active)

    Note over CLI,IAM: Phase 2: Workload Identity Configuration
    CLI->>IAM: Create GSA: dist-tx-workload@project.iam.gserviceaccount.com
    CLI->>IAM: Grant roles: cloudsql.client, pubsub.publisher, pubsub.subscriber, cloudtrace.agent
    CLI->>IAM: Bind roles/iam.workloadIdentityUser to KSA dist-tx-dev/dist-tx-engine-sa

    Note over GKE,STS: Phase 3: Runtime Pod Authentication (Zero Static Keys)
    GKE->>GKE: Pod starts with projected service account token (/var/run/secrets/...)
    GKE->>STS: Exchange KSA OpenID Connect (OIDC) token for GCP federated token
    STS->>IAM: Verify KSA-to-GSA workload identity binding
    IAM-->>STS: Signature validated
    STS-->>GKE: Return ephemeral Google OAuth2 access token (1 hour TTL)

    Note over GKE,Trace: Phase 4: Authenticated Cloud Resource Access
    GKE->>CloudSQL: Cloud SQL Auth Proxy connects with ephemeral token (:5432)
    CloudSQL-->>GKE: Mutual TLS database session established
    GKE->>PubSub: Publish transaction outbox event to topic "events"
    PubSub-->>GKE: Event ACK (MessageID: 1049281)
    GKE->>Trace: Stream Saga execution span waterfall (:443)
    Trace-->>GKE: Batch ingested
```

---

## 3. Terraform Lifecycle: Verification, Dry-Run & Production Apply

This diagram maps the CI/CD pipeline and the transition from offline dry-run verification to live production deployment.

```mermaid
graph TD
    subgraph "Tier 1: Local / CI Offline Validation Gate"
        CodeCommit["Developer commits HCL to deploy/terraform/"]
        FmtCheck["terraform fmt -check"]
        InitOffline["terraform init -backend=false"]
        Validate["terraform validate"]
        DryRunPlan["terraform plan -refresh=false -var-file=dev.tfvars"]
        
        CodeCommit --> FmtCheck --> InitOffline --> Validate --> DryRunPlan
    end

    subgraph "Tier 2: Cloud Bootstrap & State Lock"
        DryRunPlan -->|Validation Passed| RunBootstrap["scripts/run/deploy_cloud.sh"]
        RunBootstrap --> CheckBucket{"Does Remote State Bucket Exist?"}
        CheckBucket -->|No| CreateGCS["gcloud storage buckets create gs://project-dist-tx-tfstate"]
        CheckBucket -->|Yes| SyncBackend["Sync backend.tf with bucket name"]
        CreateGCS --> SyncBackend
    end

    subgraph "Tier 3: Live Terraform Infrastructure Provisioning"
        SyncBackend --> RealInit["terraform init (GCS Remote State Backend)"]
        RealInit --> RealPlan["terraform plan -out=tfplan (19 resources to create)"]
        RealPlan --> ManualApproval{"Approve Plan?"}
        ManualApproval -->|Yes| Apply["terraform apply tfplan"]
        Apply --> Output["Generate Terraform Outputs (GKE endpoint, DB conn, Redis IP)"]
    end

    subgraph "Tier 4: Kubernetes Workload Orchestration"
        Output --> GetCreds["gcloud container clusters get-credentials dev-dist-tx-gke"]
        GetCreds --> DeployK8s["kubectl apply -f deploy/k8s/base/"]
        DeployK8s --> HealthCheck["Verify Pods, Services, HPA, and OTel Collector"]
    end
```

---

## 4. Live Production Scaling: Cluster Autoscaler & HPA Under Surge

This diagram visualizes how traffic surges trigger scaling across both the compute node pool and individual application pods while protecting database connection capacity.

```mermaid
graph TB
    subgraph "Incoming Traffic Tier"
        TrafficNormal["Normal Steady Traffic (1,000 RPS)"]
        TrafficSurge["High Surge Traffic (15,000+ RPS)"]
    end

    subgraph "Google Cloud Load Balancing & Ingress"
        GLB["Google Cloud External Application Load Balancer"]
        TrafficNormal & TrafficSurge --> GLB
    end

    subgraph "GKE Cluster: dev-dist-tx-gke"
        subgraph "Node Pool: primary_nodes (e2-standard-4)"
            Node1["Node 1 (4 vCPU, 16 GB)"]
            Node2["Node 2 (4 vCPU, 16 GB)"]
            NodeAutoscaler["GKE Cluster Autoscaler (Range: 2 to 10 Nodes)"]
            NewNodeN["Autoscaled Node 3..Node 8"]
        end

        subgraph "Workload Pods: Namespace dist-tx-dev"
            HPA["Horizontal Pod Autoscaler (Target: CPU 75%, Mem 80%)"]
            Pod1["Pod 1: engine"]
            Pod2["Pod 2: engine"]
            ScaledPods["Scaled Out Pods: Pod 3..Pod 16"]
        end
    end

    subgraph "Backend Data Protection Tier"
        PgBouncer["PgBouncer Connection Pooler (Max 100 Backend Connections)"]
        CloudSQL["Cloud SQL PostgreSQL 17 (Auto-Resize Storage Enabled)"]
        Redis["Memorystore Redis 7.2 (1 GB Cache / 0.5ms Latency)"]
        PubSub["Google Cloud Pub/Sub (Partitions Automatically Handle Surge)"]
    end

    GLB --> Node1 & Node2
    HPA -->|Triggered on CPU > 75%| ScaledPods
    NodeAutoscaler -->|Unscheduled Pods Detected| NewNodeN
    NewNodeN --> ScaledPods

    Pod1 & Pod2 & ScaledPods -->|Idempotency Check| Redis
    Pod1 & Pod2 & ScaledPods -->|Batched SQL Transactions| PgBouncer --> CloudSQL
    Pod1 & Pod2 & ScaledPods -->|Asynchronous Event Ingestion| PubSub
```

---

## 5. End-to-End Cloud Observability & Distributed Tracing Fabric

This sequence diagram illustrates how telemetry generated inside GKE is collected by the OpenTelemetry Collector and forwarded to Google Cloud Observability suites (Cloud Trace, Cloud Logging, Cloud Monitoring).

```mermaid
sequenceDiagram
    autonumber
    participant App as Distributed TX Engine Pod
    participant OTel as OpenTelemetry Collector Pod (:4318)
    participant CloudTrace as Google Cloud Trace (cloudtrace.googleapis.com)
    participant CloudLogging as Google Cloud Logging (logging.googleapis.com)
    participant CloudMonitoring as Google Cloud Monitoring (monitoring.googleapis.com)
    participant SRE as DevOps / SRE Observability Dashboard

    Note over App,OTel: Step 1: In-Cluster Telemetry Generation
    App->>OTel: POST /v1/traces (Span: "saga.execute_step", trace_id: "0x7f8a9b", duration: "12ms")
    App->>OTel: POST /v1/logs (JSON: "Lock acquired for tx_8821", trace_id: "0x7f8a9b")
    App->>OTel: Scrape /metrics (Counter: "transactions_total{status='committed'}")

    Note over OTel,CloudMonitoring: Step 2: Collector Batching & GCP Export
    par Trace Export
        OTel->>CloudTrace: Export spans via gRPC with Workload Identity auth
        CloudTrace-->>OTel: Ingest ACK
    and Log Export
        OTel->>CloudLogging: Stream structured JSON entries with MonitoredResource="k8s_container"
        CloudLogging-->>OTel: Write ACK
    and Metric Export
        OTel->>CloudMonitoring: Push custom time-series metrics (custom.googleapis.com/dist_tx/rate)
        CloudMonitoring-->>OTel: Ingest ACK
    end

    Note over SRE,CloudTrace: Step 3: Investigation & Correlation in Cloud Console
    SRE->>CloudLogging: Query logs: resource.labels.namespace_name="dist-tx-dev"
    CloudLogging-->>SRE: Display log records with integrated TraceID link
    SRE->>CloudTrace: Click TraceID link to view complete distributed transaction flamegraph
    CloudTrace-->>SRE: Render waterfall showing exact latency breakdown (PostgreSQL, Redis, PubSub)
    SRE->>CloudMonitoring: View correlated dashboard showing CPU, Memory, and Node Autoscaling events
```

---

## 6. Route Tables, Cloud NAT & Outbound Egress Architecture

This diagram visualizes how private GKE worker nodes and pods with zero public IPs securely route outbound traffic through Cloud Router and Cloud NAT without allowing inbound connections.

```mermaid
graph TB
    subgraph "Private GKE Worker Nodes & Workload Pods"
        PodA["Pod A (10.20.1.15) [Pulls External SaaS API]"]
        PodB["Pod B (10.20.2.30) [Pulls Container Registry]"]
        NodeVM["GKE Node VM (10.10.0.5) [No Public External IP]"]
    end

    subgraph "VPC Routing Tier: ${environment}-dist-tx-vpc"
        VpcRouteTable["VPC Routing Table (Default Route: 0.0.0.0/0 via Default Gateway)"]
        SubnetFilter["Subnetwork Gateway (10.10.0.1)"]
    end

    subgraph "Google Cloud Managed NAT Gateway Tier"
        CloudRouter["google_compute_router: ${environment}-dist-tx-router"]
        CloudNAT["google_compute_router_nat: ${environment}-dist-tx-nat (AUTO_ONLY IPs)"]
    end

    subgraph "Public Internet"
        ExternalAPIs["External SaaS & Payment Gateways"]
        ArtifactRegistry["Google Artifact Registry / Docker Hub"]
    end

    PodA & PodB --> NodeVM
    NodeVM --> SubnetFilter --> VpcRouteTable
    VpcRouteTable --> CloudRouter --> CloudNAT
    CloudNAT -->|Source NAT with Public Regional IP| ExternalAPIs & ArtifactRegistry
```

---

## 7. Ingress Container-Native Load Balancing via Network Endpoint Groups (NEGs)

This diagram contrasts legacy two-hop NodePort routing with Google Cloud container-native NEG direct-to-pod routing.

```mermaid
graph TB
    subgraph "Client Ingress"
        ClientReq["Client HTTP/REST Request (:443)"]
        Gclb["Google Cloud External HTTP(S) Load Balancer (GLB)"]
    end

    subgraph "Legacy NodePort (2 Hops - Discarded Pattern)"
        NodeVM1["Worker Node 1 (:30080)"]
        KubeProxyIptables["kube-proxy (iptables / IPVS Rewrite + SNAT)"]
        PodHop["Pod on Worker Node 2 (Extra Inter-Node Hop)"]
    end

    subgraph "Container-Native Load Balancing via NEGs (Active Architecture)"
        Neg["Standalone Network Endpoint Group (NEG: gke-dist-tx-neg)"]
        DirectPod1["Engine Pod 1 (10.20.1.10:8080) [Direct 1-Hop]"]
        DirectPod2["Engine Pod 2 (10.20.1.11:8080) [Direct 1-Hop]"]
        DirectPod3["Engine Pod 3 (10.20.2.14:8080) [Direct 1-Hop]"]
    end

    ClientReq --> Gclb
    Gclb -.->|Avoided: 2-Hop Latency| NodeVM1 --> KubeProxyIptables --> PodHop

    Gclb ==>|Direct Packet Delivery to Pod IP via NEG| Neg
    Neg --> DirectPod1
    Neg --> DirectPod2
    Neg --> DirectPod3
```

---

## 8. Firewall Security Hierarchy & Compute Engine IAP Bastion Host Access

This diagram illustrates the multi-tier firewall rule hierarchy and how engineers connect to private Cloud SQL and Redis through the Identity-Aware Proxy (IAP) bastion host.

```mermaid
graph LR
    subgraph "Authorized Engineer Workstation"
        Dev["DevOps / SRE Engineer"]
        PsqlClient["psql localhost:5433 (IAP Tunnel)"]
    end

    subgraph "Google Cloud Edge Security & IAM Gate"
        IapGateway["Identity-Aware Proxy (IAP) TCP Tunnel (35.235.240.0/20)"]
        IamAuthCheck{"Has roles/iap.tunnelResourceAccessor?"}
    end

    subgraph "Firewall Rules Engine: ${environment}-dist-tx-vpc"
        FwIap["Rule: allow-iap-ssh (Target: 'bastion-host', Port: 22)"]
        FwHealth["Rule: allow-gcp-health-checks (Target: 'gke-node', Ports: 8080, 13133)"]
        FwInternal["Rule: allow-internal-traffic (Sources: 10.10.0.0/20, 10.20.0.0/16, 10.0.0.0/16)"]
        FwDeny["Rule: default-deny-all-ingress (Priority: 65535)"]
    end

    subgraph "Customer Subnetwork: 10.10.0.0/20"
        BastionVM["Compute Engine: dev-bastion-host (e2-micro, Zero Public IP)"]
        GkeNodes["GKE Worker Nodes (gke-node)"]
    end

    subgraph "Private Services Peering: 10.0.0.0/16"
        PrivatePG[("Cloud SQL PostgreSQL 17 (:5432)")]
        PrivateRedis[("Memorystore Redis 7.2 (:6379)")]
    end

    Dev --> PsqlClient --> IapGateway --> IamAuthCheck
    IamAuthCheck -->|Allowed| FwIap --> BastionVM
    BastionVM -->|Internal VPC Query| FwInternal --> PrivatePG & PrivateRedis
```

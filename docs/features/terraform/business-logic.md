# Terraform Infrastructure as Code — Business Logic Decision Tree

```
TERRAFORM INFRASTRUCTURE LIFECYCLE (DECISION TREE)
│
├── 1. PLAN & STATIC ANALYSIS TIER
│   └── Trigger: CI/CD Pipeline or Operator Runs `terraform plan`
│       ├── Step 1: Remote Backend State Initialization
│       │   ├── Action: Acquire State Lock in AWS DynamoDB / GCP Cloud Storage
│       │   │   ├── Lock Acquisition Fails (Concurrent Apply Running) ──> [ABORT: State Locked]
│       │   │   └── Lock Acquired ──> Read Remote State File (`terraform.tfstate`)
│       │
│       ├── Step 2: Static Security & Compliance Scanning (Checkov / tfsec / tflint)
│       │   ├── Check: Are there Open Security Groups, Unencrypted Disks, or Public S3 Buckets?
│       │   │   ├── YES ──> [FAIL PIPELINE: Security Violation Detected]
│       │   │   └── NO ──> Security Scan Passed
│       │
│       └── Step 3: Resource Spec Computation (Dry Run Diff)
│           ├── Generate Resource Change Set:
│           │   ├── Compute: Kubernetes Nodes / KinD Cluster
│           │   ├── Storage: PostgreSQL 18 Managed DB / Persistent Volume Claims
│           │   ├── Cache: Redis Cluster / ElastiCache Nodes
│           │   └── Bus: Kafka Managed Strimzi Cluster / MSK
│           └── State: PLAN_APPROVED
│
├── 2. ATOMIC PROVISIONING & DRIFT DETECTION TIER
│   └── State: PLAN_APPROVED
│       └── Trigger: Operator or CI/CD Executes `terraform apply -auto-approve`
│           ├── Step 1: Dependency Graph Topological Sorting (VPC -> DB -> Kube Cluster -> Engine Workload)
│           │
│           ├── Step 2: Cloud Provider API Invocations with Exponential Backoff
│           │   ├── Result: Provisioning Error (Quota Exceeded / Provider Outage)
│           │   │   └── Action: Halt Apply, Retain Partial State in Backend, Alert SRE
│           │   │
│           │   └── Result: SUCCESS
│           │       └── Action: Write Generated Infrastructure Outputs:
│           │           └── Database Endpoints, Kafka Bootstrap Brokers, Redis Sentinel IPs
│           │               └── State: INFRASTRUCTURE_READY
│
└── 3. OUTPUT BINDING & POST-DEPLOY HOOKS TIER
    └── State: INFRASTRUCTURE_READY
        ├── Action: Inject Infrastructure Outputs into Kubernetes ConfigMaps & SealedSecrets
        ├── Action: Release Remote State Lock in DynamoDB / Cloud Storage
        └── Action: Trigger Automated Smoke Test Verification (e.g. `./scripts/run/k8s.sh test`)
            ├── Smoke Test Fails ──> Trigger Rollback or Mark Infrastructure Degraded
            └── Smoke Test Passes ──> State: DEPLOYED ──> [TERMINAL: Infrastructure Stable & Verified]
```

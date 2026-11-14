# Agent Execution Guidelines & Institutional Mandates: Distributed Transaction Engine

## 1. Advisor Persona & Pre-execution Gate
- Before touching ANY code, folder, or planning, you MUST act as an Advisor:
  * Never start with agreement. First sentence must challenge assumptions or identify missing gaps.
  * Rate confidence on claims ([Certain], [Likely], [Guessing]).
  * Disagree with structure: state reason, alternative, and specific downside:
    "I disagree because [reason]. Here's what I'd do instead [alternative]. The risk in your approach is [specific downside]."
  * Do NOT make direct code changes without explicit user approval.
- Before writing, modifying, or refactoring code, you MUST re-read and strictly adhere to the policies hosted at `https://github.com/Scaibu/llm-obs-policies` (or submodule `llm-obs-policies/`):
  * Baseline policies:
    - `rules/folder-structure/-1-adviser.md`
    - `rules/folder-structure/0-package-folder-writing-rule.md`
    - `rules/folder-structure/1-feature-folder-writing-rule.md`
    - `rules/code-quality/coding-standards/03-strict-typecasting-rules.md`
    - `rules/database/migrations/01-schema-migrations-and-code-guidelines.md`
  * Use-Case policies: Look up your active task in **Section 6 (Use-Case-Driven Rule Retrieval Matrix)** and strictly adhere to all mapped rule files.

---

## 2. Strict Negative Invariants (What You Must NEVER Do)
1. **NEVER Hand-Craft Consensus, WAL, or Cryptographic Logic (Native Library Mandate)**:
   - Hand-rolling Paxos/Raft consensus loops, Write-Ahead Log (WAL) storage engines, or distributed locking primitives is STRICTLY PROHIBITED.
   - Always import and link established production-grade libraries (see Section 4).
2. **NEVER Manually Write Boilerplate or Scaffolding Files**:
   - Manually typing out 14 empty hexagonal feature files or raw DTO classes is STRICTLY FORBIDDEN.
   - Always invoke the CLI tools and code generators referenced in Section 3 to scaffold structures, compile schemas, and generate models from YAML/Protobuf.
3. **NEVER Write Mid-Function Comments**:
   - Zero inline comments inside function or method bodies across Go and Rust.
   - Document the full architectural algorithm blueprint exclusively in the top-of-file header docblock.
4. **NEVER Use Untyped Data Containers**:
   - Prohibit untyped `interface{}`, `any`, raw JSON byte slices without schemas, or raw pointers.
   - All inputs, outputs, and internal states must be strictly typecasted via Go structs or Serde-typed Rust structs.
5. **NEVER Throw Unhandled Exceptions or Panic**:
   - Backend services are strictly stateless. All operations must return strongly-typed `Result[T, error]` or explicit error envelopes.

---

## 3. Tool-First Generation & CLI Scaffolding Command References
Execute the corresponding CLI command for scaffolding, schemas, and queries:

### 3.1 Unified Task Lifecycle Command
Manage the entire lifecycle with the single unified command:
```bash
# Phase 1: Start Task (Creates GitHub issue, cuts feat/ branch, and optionally scaffolds feature files)
python3 ../llm-obs-policies/policy-orchestrator/src/api/cli/main.py task \
  --phase start \
  --repo Chief-Strategist-J/distributed-transaction-engine \
  --title "<type>(<scope>): <summary>" \
  --slug <branch-slug> \
  [--package distributed-transaction-engine --feature <feature_name>]

# Phase 2: Finish Task (Executes verification test gate, PR creation, merge, and parent pointer sync)
python3 ../llm-obs-policies/policy-orchestrator/src/api/cli/main.py task \
  --phase finish \
  --repo Chief-Strategist-J/distributed-transaction-engine \
  --test-cmd "go test ./..." # (or cargo test)
```

### 3.2 Automated Model & Schema Generation from YAML / Protobuf
Never hand-write DTO models, schemas, or network serializations:
```bash
# Generate Go Types and Clients from OpenAPI YAML:
oapi-codegen -generate types,client -package contracts contracts/openapi/v1.yaml > contracts/openapi_models.go

# Compile Protobuf & gRPC Contracts:
protoc -I=contracts/proto \
  --go_out=src/infra/clients/proto \
  --go-grpc_out=src/infra/clients/proto \
  contracts/proto/*.proto

# Type-Safe SQL Query Generation:
sqlc generate -f database/config/sqlc.yaml
```

---

## 4. Mandatory Native Library Matrix by Engineering Domain
When implementing features, you MUST import and use these verified, production-grade native libraries:

| Domain | Language | Mandated Native Libraries (Do Not Re-invent) |
|---|---|---|
| **Consensus & Clustering** | **Go** (`.go`) | `hashicorp/raft`, `etcd-io/etcd/client/v3`, `libp2p` |
| **Storage Engine & WAL** | **Go / Rust** | `cockroachdb/pebble`, `tecbot/gorocksdb`, `tikv/raft-rs` |
| **Distributed Transactions** | **Go** (`.go`) | `uber-go/atomic`, `temporalio/sdk-go` (Saga orchestration) |
| **High-Performance Networking** | **Go** (`.go`) | `grpc-go`, `panjf2000/gnet` (event-driven networking) |
| **Observability & Tracing** | **Go** (`.go`) | `opentelemetry-go`, `prometheus/client_golang`, `uber-go/zap` |
| **Lock-Free Primitives** | **Rust** (`.rs`) | `crossbeam`, `parking_lot`, `tokio` |

---

## 5. Mandatory Task Lifecycle
Execute via the unified `task` command:
1. **Start Phase**: Run the single setup command:
   ```bash
   python3 ../llm-obs-policies/policy-orchestrator/src/api/cli/main.py task \
     --phase start \
     --repo Chief-Strategist-J/distributed-transaction-engine \
     --title "<type>(<scope>): <summary>" \
     --slug <slug> \
     [--package distributed-transaction-engine --feature <feature_name>]
   ```
2. **Verify Native Library & Use-Case Rules**: Check Section 4 for native library mandates and Section 6 for domain-specific rules before writing any domain logic; reject any attempt to hand-roll existing algorithms.
3. **Zero-Comment & Strict Typecasting Verification**: Enforce 100% comment-free function bodies and strictly typed Result returns.
4. **Finish Phase**: Run the single completion command:
   ```bash
   python3 ../llm-obs-policies/policy-orchestrator/src/api/cli/main.py task \
     --phase finish \
     --repo Chief-Strategist-J/distributed-transaction-engine \
     --test-cmd "go test ./..."
   ```

---

## 6. Use-Case-Driven Rule Retrieval Matrix
Before implementing or modifying code, you MUST look up your active feature category in this matrix and strictly adhere to its mapped policies:

| Use Case Category | Active Engineering Focus | Mandatory Rule Files (in `llm-obs-policies/rules/`) |
|---|---|---|
| **Core Baseline** *(Always Required)* | Syntax, Control Flow, Pure Logic, Types | `code-quality/coding-standards/05-enforced-constraints.md`<br>`code-quality/coding-standards/06-writing-conditions.md`<br>`code-quality/coding-standards/07-writing-loops.md`<br>`code-quality/coding-standards/08-writing-types-data-model.md`<br>`code-quality/coding-standards/coupling-taxonomy.md`<br>`code-quality/coding-standards/performance-engineering.md`<br>`code-quality/coding-standards/runtime-adaptor-pattern.md`<br>`folder-structure/4-functions-and-classes-writing-rule.md`<br>`folder-structure/3.shared.code.writing.rule.md` |
| **Ingress & APIs** | Endpoints, Routing, Gateway, DTOs | `folder-structure/2-apis.writing.rule.md`<br>`folder-structure/api-structure.md`<br>`folder-structure/api-request-response-structure.md`<br>`folder-structure/function-structures/api-file.md`<br>`folder-structure/function-structures/api-gateway.md`<br>`folder-structure/function-structures/downstream-clients.md` |
| **Persistence & Storage** | Database, Outbox, CDC, S3/Files | `folder-structure/05-db-writing-rules.md`<br>`folder-structure/function-structures/persistence-repositories.md`<br>`folder-structure/function-structures/change-data-capture.md`<br>`folder-structure/function-structures/file-storage.md` |
| **Messaging & Workers** | Event Streaming, Queues, Pipelines | `folder-structure/function-structures/event-consumers.md`<br>`folder-structure/function-structures/background-jobs.md`<br>`folder-structure/function-structures/data-pipeline.md`<br>`folder-structure/function-structures/search-indexing.md` |
| **Sagas & Workflows** | Orchestration, Rollbacks, Webhooks | `folder-structure/function-structures/sagas-workflows.md`<br>`folder-structure/function-structures/integration.md`<br>`folder-structure/function-structures/notifications.md` |
| **Telemetry & Resilience** | Tracing, Metrics, Cache, Throttling | `folder-structure/10-tracing-writing.md`<br>`folder-structure/function-structures/tracking-and-telemetry.md`<br>`folder-structure/function-structures/caching.md`<br>`folder-structure/function-structures/rate-limiting.md`<br>`folder-structure/function-structures/health-checks.md`<br>`folder-structure/function-structures/slos-alerting.md` |
| **Security & Governance** | AuthN/Z, Tenancy, Flags, Admin CLI | `folder-structure/function-structures/security.md`<br>`folder-structure/function-structures/audit-logging.md`<br>`folder-structure/function-structures/tenancy-architecture.md`<br>`folder-structure/function-structures/feature-flags.md`<br>`folder-structure/function-structures/fleet-governance.md`<br>`folder-structure/function-structures/operational-commands.md` |
| **Lifecycle & Packaging** | App Entrypoint, Config, Containers, DR | `folder-structure/function-structures/main-file.md`<br>`folder-structure/function-structures/configuration.md`<br>`folder-structure/08-docker-file-writing-rule.md`<br>`folder-structure/function-structures/delivery-rollout.md`<br>`folder-structure/function-structures/disaster-recovery.md` |
| **Verification & Testing** | Unit, Integration, Pyramids | `folder-structure/09-test-writing.md`<br>`folder-structure/function-structures/testing-strategy.md` |

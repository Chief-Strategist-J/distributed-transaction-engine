# Authentication & Authorization — Business Logic Decision Tree

```
AUTHENTICATION & AUTHORIZATION (BUSINESS DECISION TREE)
│
├── 1. IDENTITY AUTHENTICATION TIER
│   └── Trigger: Inbound Request (HTTP/gRPC/Kafka Event) Enters Gateway/Ingress
│       ├── Protocol: Inbound gRPC / Internal Service-to-Service
│       │   └── Check: Mutual TLS (mTLS) Client Certificate Verified by SPIFFE/SPIRE?
│       │       ├── NO ──> [REJECT: gRPC Status UNAUTHENTICATED — Invalid/Missing Peer Certificate]
│       │       └── YES ──> Action: Extract SPIFFE ID (e.g. spiffe://cluster.local/ns/dist-tx/sa/engine)
│       │           └── State: PEER_AUTHENTICATED
│       │
│       └── Protocol: Inbound HTTP / Public Client API
│           ├── Check: Is Authorization Header Present (Bearer <JWT> / Api-Key <Token>)?
│           │   ├── NO ──> [REJECT: HTTP 401 Unauthorized — Missing Authorization Header]
│           │   └── YES ──> Check: Validate Token Signature & Expiry against OIDC Provider (JWKS Cache)
│           │       ├── Check: Is JWKS Cache Valid & Key ID (kid) Found?
│           │       │   ├── NO ──> Fetch Latest JWKS from IdP Endpoint
│           │       │   │   ├── Fails / Timeout ──> [REJECT: HTTP 503 Identity Provider Unavailable]
│           │       │   │   └── Succeeds ──> Refresh local cache
│           │       │   └── YES ──> Verify RS256/ES256 Cryptographic Signature
│           │       ├── Check: Token Expired (exp < current_timestamp)?
│           │       │   ├── YES ──> [REJECT: HTTP 401 Unauthorized — Token Expired]
│           │       │   └── NO ──> Check: Token Revocation List in Redis (JTI Blocklist)?
│           │       │       ├── YES ──> [REJECT: HTTP 401 Unauthorized — Token Revoked]
│           │       │       └── NO ──> Extract Subject, Tenant ID, Roles, and Scopes
│           │       │           └── State: CALLER_AUTHENTICATED
│           │
├── 2. CONTEXTUAL AUTHORIZATION & RBAC TIER
│   └── State: CALLER_AUTHENTICATED / PEER_AUTHENTICATED
│       ├── Check: Is Action Route Protected by Required Permissions (e.g. 'tx:cancel', 'tx:redrive')?
│       │   ├── Match: User Roles / Scopes Contain Required Capability
│       │   │   └── Check: Does Target Resource Belong to Caller's Tenant ID (Multi-Tenant Fence)?
│       │   │       ├── NO (Cross-Tenant Access Attempt) ──> [REJECT: HTTP 403 Forbidden — Tenant Isolation Breach]
│       │   │       └── YES (Same Tenant) ──> State: AUTHORIZED ──> [Dispatch Request to Feature Pipeline]
│       │   └── Match: User Lacks Capability
│       │       └── [REJECT: HTTP 403 Forbidden — Insufficient RBAC Permissions]
│       │
└── 3. AUDIT & RATE LIMIT ENFORCEMENT TIER
    ├── Check: Has Tenant Identity Exceeded Dynamic Call Quota (Token Bucket in Redis)?
    │   ├── YES ──> [REJECT: HTTP 429 Too Many Requests — Retry-After Header Injected]
    │   └── NO ──> Action: Decrement Token Bucket & Emit Ingress Audit Span
    │       └── State: ALLOWED ──> [TERMINAL: Handover to Business Handler]
```

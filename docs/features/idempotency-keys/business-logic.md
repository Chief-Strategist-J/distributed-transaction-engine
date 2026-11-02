# Idempotency Keys — Business Logic Decision Tree

```
IDEMPOTENCY KEYS (BUSINESS DECISION TREE)
│
├── 1. VALIDATION & FINGERPRINTING TIER
│   └── Trigger: Inbound Transaction Request Received by API Ingress
│       ├── Check: Is `Idempotency-Key` Header Provided by Client?
│       │   ├── NO ──> [REJECT: HTTP 400 Bad Request — Missing Idempotency-Key Header]
│       │   └── YES ──> Action: Validate Format (UUIDv4 or Valid RFC Format, Length <= 128 chars)
│       │       ├── INVALID ──> [REJECT: HTTP 400 Bad Request — Malformed Idempotency-Key]
│       │       └── VALID ──> Action: Compute Cryptographic Request Hash:
│       │           └── RequestFingerprint = SHA256(ClientTenantID || Route || RawRequestBody)
│       │               └── State: KEY_PARSED
│
├── 2. FAST-PATH CACHE & LOCK ACQUISITION TIER
│   └── State: KEY_PARSED
│       └── Action: Atomically Evaluate Key in Redis via Lua Script:
│           ├── Branch A: Key Exists with status='COMPLETED'
│           │   ├── Check: Does Stored Fingerprint == RequestFingerprint?
│           │   │   ├── YES (Legitimate Replay)
│           │   │   │   └── Action: Retrieve Cached Response (Status Code, Headers, Body)
│           │   │   │       └── State: CACHED_REPLAY ──> [TERMINAL: Return Replayed HTTP Response with `X-Cache: HIT`]
│           │   │   └── NO (Payload Mismatch)
│           │   │       └── [REJECT: HTTP 422 Unprocessable Entity — Idempotency Key Reused with Different Payload]
│           │
│           ├── Branch B: Key Exists with status='PROCESSING' (In-Flight)
│           │   └── [ABORT: HTTP 409 Conflict — Concurrent Request In-Flight with Identical Idempotency Key]
│           │
│           └── Branch C: Key Does NOT Exist
│               └── Action: Acquire Redis Mutex Lock (SET key status='PROCESSING' EX 120 NX)
│                   ├── Lock Acquired ──> State: KEY_CLAIMED ──> Advance to Execution Tier
│                   └── Lock Collision (Concurrent Arrival) ──> [ABORT: HTTP 409 Conflict]
│
└── 3. RECORD FINALIZATION & PERSISTENCE TIER
    └── State: KEY_CLAIMED
        ├── Action: Dispatch Transaction to Core Saga Execution Engine
        │   ├── Execution Result: SUCCESS (HTTP 200 / 201)
        │   │   ├── Action: Atomically Persist Response in PostgreSQL `idempotency_records`
        │   │   └── Action: Update Redis Key (status='COMPLETED', payload=Response, TTL=86400s)
        │   │       └── State: STORED ──> [TERMINAL: Return Fresh HTTP Response to Client]
        │   │
        │   └── Execution Result: INFRASTRUCTURE / TRANSIENT ERROR (HTTP 500 / 503)
        │       └── Action: Delete Redis Mutex Key (Allow Client to Retry Immediately)
        │           └── State: KEY_RELEASED ──> [TERMINAL: Return Error Response to Client]
```

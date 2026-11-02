# Compensation Retry — Business Logic Decision Tree

```
COMPENSATION RETRY (BUSINESS DECISION TREE)
│
├── 1. FAILURE CLASSIFICATION TIER
│   └── Trigger: Compensation Action Dispatch Returns Error or Network Timeout
│       ├── Check: Is Error Explicitly Uncompensable (e.g. HTTP 400 Bad Schema, Permanent Auth Revoked)?
│       │   ├── YES ──> [IMMEDIATE ESCALATION: Halt retry loop immediately ──> Transition to State: STUCK]
│       │   └── NO ──> Check: Is Downstream Service Protected by Open Circuit Breaker?
│       │       ├── YES (Circuit Open) ──> Action: Delay retry until circuit half-open probe succeeds
│       │       └── NO (Circuit Closed/Half-Open) ──> State: RETRY_ELIGIBLE
│
├── 2. BACKOFF CALCULATION & BUDGET EVALUATION TIER
│   └── State: RETRY_ELIGIBLE
│       ├── Action: Increment step compensation_attempts counter (Current Attempt = N)
│       ├── Check: Has Current Attempt Exceeded Max Retry Threshold (Default: 10 Attempts)?
│       │   ├── YES ──> Action: Exhausted retry budget
│       │   │   └── Action: Mark step compensation_status='EXHAUSTED'
│       │   │       └── State: STUCK ──> [TERMINAL: Route to Dead-Letter Quarantine & Page SRE]
│       │   │
│       │   └── NO ──> Action: Calculate Backoff Duration via Full Jitter Algorithm:
│       │       │       SleepDuration = Random(0, Min(MaxBackoff, BaseInterval * 2^N))
│       │       │
│       │       └── Action: Check Tenant Dynamic Retry Budget:
│       │           ├── Check: Has Tenant consumed > 20% of shared system retry bandwidth?
│       │           │   ├── YES ──> Action: Throttle and clamp backoff to MaxInterval (Deprioritize)
│       │           │   └── NO ──> Action: Schedule Retry Execution Event in TimeWheel Scheduler
│       │           └── State: WAITING_FOR_RETRY
│
└── 3. REDRIVE & EXECUTION TIER
    └── State: WAITING_FOR_RETRY
        └── Trigger: Backoff Timer Fires
            ├── Action: Verify Active Saga State in DB is still 'COMPENSATING'
            │   ├── NO (Operator Manually Cancelled or Force-Resolved) ──> [ABORT: Discard Stale Retry]
            │   └── YES ──> Action: Acquire Redis Step Lock & Re-dispatch Compensating HTTP/gRPC Request
            │       ├── Outcome: SUCCESS
            │       │   └── Action: Reset step failure counter ──> Return control to Compensation Engine
            │       └── Outcome: RE-FAILED
            │           └── Loop back to Tier 1 with Attempt N+1
```

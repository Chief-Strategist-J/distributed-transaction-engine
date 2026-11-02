# Retry Policies — Business Logic Decision Tree

```
RETRY POLICIES (BUSINESS DECISION TREE)
│
├── 1. ERROR CLASSIFICATION TIER
│   └── Trigger: Remote Step Execution Fails with Error or Timeout
│       ├── Action: Inspect Error Category & HTTP/gRPC Status Code:
│       │   ├── Class A: RETRYABLE TRANSIENT ERROR
│       │   │   ├── HTTP: 408 (Timeout), 429 (Too Many Requests), 500 (Internal), 502 (Bad Gateway), 503 (Unavailable), 504 (Timeout)
│       │   │   ├── gRPC: DEADLINE_EXCEEDED, UNAVAILABLE, RESOURCE_EXHAUSTED
│       │   │   └── Transport: TCP Connection Reset, Broken Pipe, DNS Resolution Timeout
│       │   │   └── State: CLASSIFIED_RETRYABLE ──> Proceed to Tier 2
│       │   │
│       │   ├── Class B: NON-RETRYABLE BUSINESS REJECTION
│       │   │   ├── HTTP: 400 (Bad Request), 401 (Unauthorized), 403 (Forbidden), 404 (Not Found), 422 (Unprocessable)
│       │   │   ├── gRPC: INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS, PERMISSION_DENIED
│       │   │   └── Action: Immediate Failure without Retry
│       │   │       └── State: TERMINAL_STEP_FAILURE ──> [Halt Step & Transition to Compensating Tier]
│       │   │
│       │   └── Class C: UNKNOWN / INDETERMINATE ERROR (e.g. Connection dropped mid-response)
│       │       └── Action: Query Target Service via Idempotency Key to probe actual state before retrying
│
├── 2. BUDGET & STRATEGY EVALUATION TIER
│   └── State: CLASSIFIED_RETRYABLE
│       ├── Check: Has Step Attempts Counter Reached `policy.max_attempts` (e.g. 5)?
│       │   ├── YES ──> [BUDGET EXHAUSTED: Mark Step as Failed ──> Transition to Compensation Engine]
│       │   └── NO ──> Check: Has Tenant Retry Token Bucket Capacity Available?
│       │       ├── NO ──> [RETRY RATE-LIMITED: Postpone or Fail Fast to Protect Downstream]
│       │       └── YES ──> Action: Decrement Tenant Retry Token Bucket
│
└── 3. JITTERED BACKOFF CALCULATION & EXECUTION TIER
    ├── Action: Select Backoff Algorithm Specified in Workflow:
    │   ├── Option 1: EXPONENTIAL_FULL_JITTER (Default)
    │   │   └── Backoff = Random(0, Min(MaxInterval, BaseInterval * 2^Attempt))
    │   ├── Option 2: EXPONENTIAL_EQUAL_JITTER
    │   │   └── Half = Min(MaxInterval, BaseInterval * 2^Attempt) / 2
    │   │   └── Backoff = Half + Random(0, Half)
    │   └── Option 3: LINEAR_BACKOFF
    │       └── Backoff = Min(MaxInterval, BaseInterval * Attempt)
    │
    └── Action: If HTTP 429, Check for `Retry-After` Header from Target:
        ├── Header Present: Use Max(ComputedBackoff, HeaderDuration)
        └── Header Absent: Use ComputedBackoff
            └── Action: Schedule Execution in Engine TimeWheel Worker
                └── State: RETRY_SCHEDULED ──> [Wait Duration & Re-dispatch Step]
```

Your authorization stops at "**has the permission** and **is in the same tenant**." Inside a tenant, any user holding `tx:cancel` can cancel **any** saga: a colleague's payment, another department's payout, a transaction above their approval limit.

Real authorization also needs:
- **Resource-level checks:** this user owns or is assigned this transaction.
- **State rules:** cancel is impossible after the pivot; redrive needs a fixed cause.
- **Risk controls:** amount limits, step-up authentication, a second approver for money actions.

Role plus tenant is the coarsest layer, not the decision. [Certain]

Five more errors, ranked by damage:

**1. Authenticated services are never authorized.** [Certain]
I disagree because mTLS proves *which workload* is calling, but the tree then checks "user roles/scopes," which services don't have. As written, **any workload in the mesh** with a valid SPIFFE certificate can call `Redrive` or `ForceComplete` on the engine.
Here's what I'd do instead: an explicit policy mapping (caller SPIFFE ID → method), default deny. Services acting for users also carry an audience-restricted on-behalf-of token.

**2. JWT validation checks only signature and expiry, and the unknown-`kid` path is a denial-of-service lever.** [Certain]
I disagree because missing checks enable well-known attacks:
- **Algorithm allow-list:** without it, `alg: none` and RSA/HMAC confusion are possible.
- **Issuer and audience:** without them, tokens issued for other applications are accepted.
- **Token type:** without it, ID tokens are accepted as access tokens.
- **`nbf` and clock skew:** not handled.

Separately, fetching the JWKS on every unknown `kid` lets an attacker send random `kid` values and drive a **request storm at your identity provider**.

**3. The revocation check has no failure policy.** [Certain]
I disagree because when Redis is unavailable, the code either skips the check (revoked tokens accepted) or rejects everything (total outage). Neither should be accidental.
Here's what I'd do instead: decide per action risk. Low-risk reads tolerate a short revocation lag. Money and admin actions fail closed or check the identity provider directly.

**4. Kafka events and provider webhooks don't fit this flow at all.** [Certain]
I disagree because events carry no caller token. Their provenance comes from producer ACLs, and the envelope's actor field is a **record**, not a credential.
Provider callbacks (payment confirmations) authenticate by **request signature with replay protection** (timestamp and nonce), not JWTs. A replayed "payment succeeded" webhook moves a saga forward twice.

**5. Authorization decisions go into a sampled trace span, and 403 leaks existence.** [Certain]
Traces are sampled and dropped. Denials and privileged allows belong in the **audit trail**.
"403 Tenant Isolation Breach" confirms that the resource exists in another tenant. Return **404**.

```
AUTHENTICATION & AUTHORIZATION (SAGA ENGINE) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (verify per IdP/library)   [G] Guessing (security/business must decide)
│
├── 0. CALLER TYPES  (each has its own authentication mechanism)
│   ├── End users (browser/mobile) → OIDC access tokens (JWT)
│   ├── API clients (tenant integrations) → API keys or OAuth client credentials
│   ├── Internal services → mTLS workload identity (SPIFFE) + internal on-behalf-of token when acting for a user
│   ├── Operators (support/SRE/finance) → OIDC + step-up MFA for privileged actions; actions via control plane
│   ├── External providers (payment/supplier callbacks) → signed requests with replay protection
│   └── Kafka producers → broker authentication + write ACLs (provenance); no per-message credentials
│
├── 1. INVARIANTS
│   ├── AA-01 Every request is authenticated by the mechanism of its caller type; no fallback to weaker mechanisms [C]
│   ├── AA-02 Authorization = permission ∧ tenant scope ∧ resource relationship ∧ state rules ∧ risk controls [C]
│   ├── AA-03 Service identities are authorized per method by explicit policy; default deny [C]
│   ├── AA-04 Token validation enforces algorithm allow-list, issuer, audience, token type, exp/nbf with bounded skew [C]
│   ├── AA-05 High-risk actions use fresh authorization (not token-embedded roles) and step-up/approvals as defined [C]
│   ├── AA-06 Failure of identity/revocation/policy components follows a declared policy per action risk [C]
│   ├── AA-07 Denials and privileged allows are written to the audit trail [C]
│   ├── AA-08 Responses don't reveal existence or ownership of resources the caller can't access [C]
│   └── AA-09 Provider callbacks are authenticated by signature and protected against replay [C]
│
├── 2. PRE-AUTHENTICATION PROTECTIONS
│   ├── Rate limit per client IP / API key prefix / client id BEFORE expensive validation
│   ├── Strip client-supplied internal identity/tenant headers
│   ├── Header and body size limits
│   └── E2.1 Unauthenticated flood forces JWKS fetches and signature checks — IdP and CPU exhaustion [C]
│
├── 3. AUTHENTICATION
│   ├── 3.1 JWT (users, OAuth clients)  (AA-04)
│   │   ├── Algorithm in allow-list (asymmetric only); header "alg" never trusted to choose verification method
│   │   ├── Key lookup by kid in cached JWKS
│   │   │   ├── Known kid → verify
│   │   │   ├── Unknown kid → single-flight JWKS refresh, rate-limited [L: ≤ 1/min per issuer] → still unknown → 401
│   │   │   └── JWKS endpoint down → keep using cached keys up to max staleness [L: hours]; unknown kids → 401
│   │   ├── Issuer exact match; audience contains this API; token type = access token
│   │   ├── exp, nbf, iat with bounded clock skew [L: ≤ 60 s]
│   │   ├── Required claims present: subject, tenant/memberships, scopes
│   │   └── Edge cases
│   │       ├── E3.1a ID token presented as access token — accepted — SECURITY [C]
│   │       ├── E3.1b Random kid per request — JWKS fetch storm — IdP OUTAGE [C]
│   │       ├── E3.1c IdP rotates keys without overlap — valid tokens rejected — OUTAGE [C]
│   │       └── E3.1d Gateway clock skew — valid tokens rejected / expired accepted [C]
│   │
│   ├── 3.2 API keys
│   │   ├── Key = public prefix + secret; secret stored hashed; lookup by prefix, constant-time compare
│   │   ├── Key bound to tenant, scopes, optional IP/CIDR restrictions, expiry
│   │   ├── Revocation immediate (no long cache) [L: ≤ seconds]
│   │   ├── Rotation: two active keys per client during overlap
│   │   └── E3.2a Keys stored in plaintext or logged — mass credential exposure — SECURITY [C]
│   │
│   ├── 3.3 Internal services
│   │   ├── mTLS with workload identity verification (trust domain, path)
│   │   ├── When acting for a user/tenant: internal token (audience = callee, short TTL, delegation chain) bound to caller identity
│   │   └── E3.3a Service forwards end-user JWT to all services — replayable across services — SECURITY [C]
│   │
│   ├── 3.4 Operators
│   │   ├── OIDC with MFA; privileged actions require recent step-up (re-authentication within window) [G]
│   │   └── Break-glass identities: personal, time-bound, audited
│   │
│   ├── 3.5 Provider callbacks / webhooks  (AA-09)
│   │   ├── Verify provider signature over raw body with provider secret/public key
│   │   ├── Timestamp within tolerance [L: ≤ 5 min] + nonce/event id deduplication
│   │   ├── Callback content treated as a claim → verify with provider API for money-moving transitions [G]
│   │   └── Edge cases
│   │       ├── E3.5a Replayed "payment succeeded" callback — saga advances twice / duplicate fulfilment [C]
│   │       └── E3.5b Signature verified on re-serialized body — legitimate callbacks rejected or forged accepted [C]
│   │
│   ├── 3.6 Kafka messages
│   │   ├── Producer authentication at broker; topic write ACLs = provenance
│   │   ├── Envelope actor = record of who caused the event, never a credential for new privileged actions
│   │   └── E3.6a Consumer grants privileges based on envelope "actor" field — forged/replayed events escalate [C]
│   │
│   └── 3.7 Revocation  (AA-06)
│       ├── Sources: token revocation (jti/session), user disabled, key revoked, membership removed
│       ├── Store unavailable
│       │   ├── Low-risk reads → accept with short revocation lag (bounded by token TTL) [G]
│       │   └── Money/admin actions → fail closed or verify via IdP introspection
│       └── E3.7a Revocation store down; checks skipped silently — revoked admin keeps acting — SECURITY [C]
│
├── 4. AUTHORIZATION DECISION  (AA-02)
│   └── For each request after authentication
│       ├── Layer 1: Action permission (scope/role grants action "tx:cancel")
│       │   └── Missing → 403 (action-level; doesn't reveal resource)
│       ├── Layer 2: Tenant scope — resource tenant ∈ caller's verified memberships
│       │   └── Mismatch → 404 + security event (AA-08)
│       ├── Layer 3: Resource relationship (within tenant)
│       │   ├── Ownership/assignment/team/cost-center rules [G: per resource type]
│       │   └── Not related → 404 (or 403 if existence already known to caller by policy)
│       ├── Layer 4: State-dependent business rules
│       │   ├── e.g., cancel only before pivot; modify only in editable states; redrive only for eligible DLQ records
│       │   └── Violated → 409 with business reason (not a security event)
│       ├── Layer 5: Risk controls
│       │   ├── Amount limits per role/user [G]
│       │   ├── Step-up authentication required (recent MFA)
│       │   └── Second approver required (money actions, write-offs, force-complete) — requester ≠ approver
│       ├── Service callers (AA-03): caller SPIFFE ID allowed for this method; plus on-behalf-of user evaluated through layers 1–5
│       └── Edge cases
│           ├── E4.1 User with tx:cancel cancels any saga in tenant — intra-tenant IDOR — SECURITY [C]
│           ├── E4.2 Any mesh workload calls Redrive/ForceComplete — no method policy — SECURITY [C]
│           ├── E4.3 Bulk endpoint authorizes the batch once — some items not permitted — SECURITY [C]
│           │   └── Handling: authorize each item
│           └── E4.4 Authorization uses read-model attributes (stale owner) — wrong decision [C]
│               └── Handling: resource attributes for decisions read from authoritative store
│
├── 5. SAGA ACTION MATRIX  (examples; business must finalize) [G]
│   ├── Start saga (create payment/order) → user/client with scope; tenant; amount limit; idempotency key required
│   ├── View saga → owner or role with tenant-wide read; masked fields per entitlement
│   ├── Cancel saga → owner or approver role; pre-pivot only; amount threshold → approval
│   ├── Modify parameters → owner; editable states only; expected version required
│   ├── Retry step / redrive DLQ → operator role; eligibility checks; money → second approver
│   ├── Resolve step / force complete / write-off → operator + second approver; evidence required
│   ├── Create/modify schedules → scope + run-as principal checks at each firing
│   └── Publish/promote workflow versions → engineering roles; separate publisher and approver
│
├── 6. FRESHNESS & CACHING  (AA-05)
│   ├── Token-embedded roles acceptable for low-risk actions (bounded by token TTL)
│   ├── High-risk actions query the policy decision point / membership store fresh
│   ├── Decision cache key = (principal, tenant, action, resource id, policy version); short TTL; invalidated on role change
│   └── E6.1 Role removed but token still valid for 1 hour — redrive/cancel still allowed — SECURITY [C]
│
├── 7. COMPONENT FAILURE POLICIES  (AA-06)
│   ├── IdP down → existing valid tokens accepted (by design); new logins fail; operator break-glass per policy
│   ├── JWKS unreachable → cached keys until max staleness, then reject
│   ├── Policy decision point down → cached decisions for low-risk; fail closed for mutations/money
│   ├── Revocation store down → §3.7 policy
│   └── E7.1 Policy service outage silently allows everything (fail open by default) — SECURITY [C]
│
├── 8. RESPONSES & LEAKAGE  (AA-08)
│   ├── 401: authentication failed (no detail on which check)
│   ├── 403: action not permitted for caller in general
│   ├── 404: resource not visible (other tenant, not related, or nonexistent — indistinguishable)
│   ├── 409: business state rule
│   ├── 429: rate limited (Retry-After)
│   └── E8.1 Error body explains "belongs to tenant B" — ownership leak [C]
│
├── 9. DELAYED & ASYNCHRONOUS ACTIONS
│   ├── Authorization at command time binds the business fact (saga start, cancel request)
│   ├── Scheduled runs re-authorize the run-as principal at firing
│   ├── Deliveries of data to people (exports, notifications with personal data) re-check entitlement at delivery
│   ├── Compensations triggered by the system run under system authority, recorded with the triggering cause
│   └── E9.1 Export queued before user's access revoked, delivered after — data to unauthorized person [C]
│
├── 10. AUDIT OF DECISIONS  (AA-07)
│   ├── Denials (with reason class), privileged allows (approvals, step-up), break-glass sessions → audit trail
│   ├── Cross-tenant attempts → security events
│   ├── Traces may carry decision ids for correlation, but are never the record
│   └── E10.1 Authorization evidence only in sampled spans — missing in investigation [C]
│
└── 11. FEATURE TEST SCENARIOS
    ├── alg "none" / HMAC-with-public-key → rejected
    ├── ID token as access token → rejected; wrong audience → rejected
    ├── 10,000 requests with random kid → JWKS refreshed at most per rate limit; IdP unaffected
    ├── IdP key rotation → no 401 spike
    ├── User in tenant A cancels colleague's saga without relationship → 404
    ├── Cancel after pivot → 409 business reason
    ├── Force-complete without second approver → rejected
    ├── Mesh workload not in policy calls Redrive → denied
    ├── Service acting for user → user's permissions evaluated (not service's)
    ├── Replayed provider callback → rejected (nonce/timestamp); no duplicate saga transition
    ├── Kafka event with forged actor → no privilege granted
    ├── Revocation store down → money actions fail closed; reads continue per policy
    ├── Role removed → high-risk actions denied immediately
    ├── Cross-tenant id → 404, security event; no ownership detail
    └── Denials and privileged approvals present in audit trail (not only traces)
```

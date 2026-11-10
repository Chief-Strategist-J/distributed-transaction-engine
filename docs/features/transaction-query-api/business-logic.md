A rich query language over transactions turns into an **unbounded, index-defeating, cross-tenant query engine** unless the language is designed around three things:
- **what your indexes can answer cheaply;**
- **what authorization can inject and enforce;**
- **what consistency you can actually promise.**

Most transaction-search incidents I'd expect here come from free-form filters that hit unindexed fields, page through millions of rows, or leak data through filters on fields the caller can't see. [Certain]

So "include everything" has to mean **everything users legitimately need, expressed through a language with hard limits**, not arbitrary expressions. Below is the full design: feature scope, the query language (grammar, types, semantics, limits, examples), the execution lifecycle with every rejection path, and the edge cases.

```
TRANSACTION-QUERY-API — BUSINESS LOGIC & QUERY LANGUAGE DECISION TREE (v1)
Legend: [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (product/security must decide)
│
├── 0. SCOPE, CONSUMERS & ENDPOINTS
│   ├── Consumers
│   │   ├── Tenant users (own/assigned transactions, tenant-wide by role)
│   │   ├── Tenant API clients (integrations)
│   │   ├── Operators / support (cross-tenant only via impersonation session)
│   │   ├── Finance / auditors (period-based, read-only, masked by default)
│   │   └── Internal services (by contract; method-level authorization)
│   ├── Endpoints
│   │   ├── GET  /transactions/{id}                 — single transaction (consistency token aware)
│   │   ├── GET  /transactions/{id}/timeline        — steps, attempts, compensations (from engine history)
│   │   ├── POST /transactions:search               — TQL filter + sort + projection + pagination
│   │   ├── POST /transactions:aggregate            — TQL filter + grouped aggregates
│   │   ├── POST /transactions:export               — async export job (large result sets)
│   │   └── Saved queries: create / list / run (stored TQL with owner and visibility)
│   └── Not in scope: mutations (commands live in command API), raw SQL/DSL passthrough, cross-tenant search for tenants
│
├── 1. INVARIANTS
│   ├── TQ-01 Tenant and resource-level authorization are injected into the query AST as mandatory predicates;
│   │        never applied as post-filters and never removable by the caller [C]
│   ├── TQ-02 Callers can filter, sort, group, or search only on fields they are entitled to see unmasked [C]
│   ├── TQ-03 Queries compile from an AST to parameterized store queries; no string concatenation, no passthrough [C]
│   ├── TQ-04 Every query passes a cost check before execution; unbounded queries are rejected, not timed out [C]
│   ├── TQ-05 Pagination is keyset-based with opaque, signed, query-bound cursors; no deep offsets [C]
│   ├── TQ-06 Every response states its consistency: store used, freshness/as-of, completeness [C]
│   ├── TQ-07 Money is never summed or compared across currencies implicitly [C]
│   ├── TQ-08 Time literals are explicit about time zone; relative times resolved once per query [C]
│   ├── TQ-09 Errors are typed, positioned, and never leak data or existence of other tenants' resources [C]
│   ├── TQ-10 Sensitive queries and exports are audited [C]
│   └── TQ-11 The language and field registry are versioned; behaviour of a version never changes silently [C]
│
├── 2. DATA SOURCES & ROUTING
│   ├── S-PRIMARY  Authoritative transaction view (PostgreSQL) — exact filters, money, status, keyset pagination
│   ├── S-SEARCH   Search index (derived) — full-text, flexible combinations, faceting
│   ├── S-HISTORY  Engine history (saga steps, attempts, compensations) — timeline endpoint
│   ├── Routing decision per query (planner)
│   │   ├── Uses only fields indexed in S-PRIMARY and no full-text → S-PRIMARY
│   │   ├── Uses full-text or search-only fields → S-SEARCH
│   │   ├── Requires exact totals for finance (aggregate with exactness=EXACT) → S-PRIMARY (or rejected if too costly)
│   │   └── Timeline → S-HISTORY
│   └── Edge cases
│       ├── E2.1 Same query returns different results from PRIMARY vs SEARCH (refresh lag) — response marks store + freshness [C]
│       └── E2.2 Search index rebuilding / degraded — planner falls back to PRIMARY for supported subset or returns 503 [C]
│
├── 3. FIELD REGISTRY  (single source of truth for the language)
│   ├── Per field: name, type, nullable, operators allowed, filterable, sortable, groupable, searchable (full-text),
│   │   stores where indexed, entitlement class (PUBLIC | INTERNAL | FINANCIAL | PII | RESTRICTED), mask rule,
│   │   deprecation status, since-version
│   ├── Example fields (illustrative)
│   │   ├── id                        string (opaque)        =, IN                    sortable(tiebreaker)
│   │   ├── status                    enum (business status) =, !=, IN, NOT IN        groupable
│   │   ├── workflow_type             enum                   =, IN                    groupable
│   │   ├── workflow_version          string                 =, IN                    INTERNAL
│   │   ├── created_at / completed_at timestamp              <, <=, >, >=, BETWEEN    sortable, groupable(bucket)
│   │   ├── amount                    money                  =, <, <=, >, >=, BETWEEN (same currency) FINANCIAL
│   │   ├── currency                  enum (ISO 4217)        =, IN                    groupable
│   │   ├── customer_ref              string (opaque)        =, IN                    PII-linked (opaque ok)
│   │   ├── customer_email            string                 = only (exact)           PII (masked; rarely filterable)
│   │   ├── merchant_ref              string                 =, IN, PREFIX            
│   │   ├── provider_reference        string                 =                        RESTRICTED
│   │   ├── correlation_id            string                 =                        INTERNAL
│   │   ├── tags                      set<string>            HAS, HAS_ANY, HAS_ALL
│   │   ├── description               text                   MATCH (full-text)        SEARCH only
│   │   ├── steps[]                   collection             ANY / ALL / NONE (nested predicate)
│   │   │   ├── steps.name, steps.status, steps.attempts, steps.last_error_class, steps.completed_at
│   │   ├── compensation_status       enum                   =, IN
│   │   └── needs_attention           boolean                =                        operators only
│   └── Edge cases
│       ├── E3.1 Field exposed in language but not indexed in routed store — full scans — TQ-04 rejects [C]
│       └── E3.2 Internal state names (NEEDS_ATTENTION, UNKNOWN) shown to tenants — leaks internals [C]
│           └── Handling: business status mapping per audience; internal states operator-only
│
├── 4. QUERY LANGUAGE (TQL)
│   │
│   ├── 4.1 Forms
│   │   ├── Text form (humans): compiled to AST
│   │   ├── JSON AST form (programs): canonical; text form always normalizes to it
│   │   └── Both versioned: "tql_version" in request (TQ-11)
│   │
│   ├── 4.2 Grammar (text form, EBNF)
│   │   query        = filter [ order_by ] [ limit ] ;
│   │   filter       = or_expr ;
│   │   or_expr      = and_expr { "OR" and_expr } ;
│   │   and_expr     = not_expr { "AND" not_expr } ;
│   │   not_expr     = [ "NOT" ] primary ;
│   │   primary      = "(" or_expr ")" | predicate ;
│   │   predicate    = comparison | in_pred | between | null_pred | text_pred | set_pred | nested ;
│   │   comparison   = field comp_op value ;
│   │   comp_op      = "=" | "!=" | "<" | "<=" | ">" | ">=" | "PREFIX" ;
│   │   in_pred      = field [ "NOT" ] "IN" "(" value { "," value } ")" ;
│   │   between      = field "BETWEEN" value "AND" value ;                (inclusive lower, exclusive upper)
│   │   null_pred    = field "IS" [ "NOT" ] "NULL" ;
│   │   text_pred    = field "MATCH" string ;                             (full-text, SEARCH store only)
│   │   set_pred     = field ( "HAS" | "HAS_ANY" | "HAS_ALL" ) set_value ;
│   │   nested       = collection ( "ANY" | "ALL" | "NONE" ) "(" or_expr ")" ;
│   │   order_by     = "ORDER BY" sort_key { "," sort_key } ;
│   │   sort_key     = field [ "ASC" | "DESC" ] [ "NULLS FIRST" | "NULLS LAST" ] ;
│   │   limit        = "LIMIT" integer ;
│   │   value        = string | number | boolean | enum | money | timestamp | relative_time | param ;
│   │   money        = decimal currency_code ;                            (e.g., 125.50 INR)
│   │   timestamp    = ISO-8601 with offset | date "@" tz_name ;          (e.g., 2026-11-01T00:00:00+05:30, 2026-11-01@Asia/Kolkata)
│   │   relative_time= "now()" [ ( "-" | "+" ) duration ] | "start_of(" unit "," tz_name ")" ;
│   │   duration     = integer ( "m" | "h" | "d" | "w" ) ;
│   │   param        = ":" identifier ;                                   (bound separately; typed by field)
│   │
│   ├── 4.3 Types & literal rules
│   │   ├── Strings: UTF-8, normalized (NFC); length limit per field [L: 256]
│   │   ├── Enums: case-sensitive canonical values; unknown value → error (not empty result)
│   │   ├── Numbers: decimal; integers for counts; no floats for money
│   │   ├── Money: decimal + currency → converted to minor units by currency exponent; precision beyond exponent → error
│   │   ├── Timestamps: must include offset or zone; "date@zone" means local midnight in that zone
│   │   ├── Relative times: resolved once at query start (server time) and returned in response metadata
│   │   └── Params: values bound by type; same safety as literals
│   │
│   ├── 4.4 Operators by type
│   │   ├── enum: =, !=, IN, NOT IN
│   │   ├── string(opaque id): =, IN (exact only)
│   │   ├── string(reference): =, IN, PREFIX (min prefix length [L: 3]) — no leading wildcards
│   │   ├── timestamp: <, <=, >, >=, BETWEEN
│   │   ├── money: =, <, <=, >, >=, BETWEEN — only with currency = X in same conjunction (TQ-07)
│   │   ├── integer: =, !=, <, <=, >, >=, BETWEEN, IN
│   │   ├── boolean: =
│   │   ├── set<string>: HAS, HAS_ANY, HAS_ALL
│   │   ├── text: MATCH (tokenized search; phrase with quotes; no regex)
│   │   └── Not supported by design: regex, leading wildcards, arbitrary functions, joins, sub-queries
│   │
│   ├── 4.5 Boolean semantics
│   │   ├── Precedence: NOT > AND > OR; parentheses override
│   │   ├── Normalization: AST flattened, constant-folded, duplicate predicates removed
│   │   └── Contradictions (status = A AND status = B) → valid query, empty result (planner short-circuits)
│   │
│   ├── 4.6 Nested collection semantics (steps[])
│   │   ├── steps ANY (name = "payment" AND status = "FAILED") — both conditions on the SAME step element [C]
│   │   ├── steps ALL (...) — every element matches (empty collection → true; documented)
│   │   ├── steps NONE (...) — no element matches
│   │   ├── Fields of a collection usable only inside ANY/ALL/NONE (prevents cross-element false matches)
│   │   └── Nesting depth limit [L: 1 level; no nested collections inside nested]
│   │
│   ├── 4.7 Time semantics
│   │   ├── Fields carry meaning: created_at (recorded), effective_at (business effective), completed_at
│   │   ├── BETWEEN on timestamps: inclusive lower, exclusive upper (no double counting across adjacent ranges)
│   │   ├── Time-bucketing for aggregates uses explicit zone (DST-aware buckets)
│   │   └── Edge: "date@zone" on DST-gap dates resolves per zone rules (start of day exists; documented)
│   │
│   ├── 4.8 Money semantics (TQ-07)
│   │   ├── amount comparisons require currency equality predicate in the same AND-group
│   │   ├── Aggregates on amount require GROUP BY currency (or single-currency filter)
│   │   └── No implicit FX conversion; converted views are separate fields with declared rate source [G]
│   │
│   ├── 4.9 Null semantics
│   │   ├── Comparisons with null fields are false (null ≠ anything); use IS NULL / IS NOT NULL
│   │   ├── != excludes nulls (documented); "status != X OR status IS NULL" if needed
│   │   └── Sorting: NULLS LAST default for ASC, NULLS FIRST for DESC [L] (explicit override allowed)
│   │
│   ├── 4.10 Limits (TQ-04)
│   │   ├── Max query text length [L: 4 KB]; max AST nodes [L: 100]; max boolean depth [L: 8]
│   │   ├── Max IN list size [L: 500]; max OR branches [L: 20]
│   │   ├── Max LIMIT per page [L: 200]
│   │   ├── Required time bound for tenants above size threshold (created_at range ≤ max window) [G]
│   │   └── Max sort keys [L: 3] + implicit id tiebreaker
│   │
│   ├── 4.11 Examples (valid)
│   │   ├── status IN ("FAILED", "COMPENSATED") AND created_at >= now()-7d ORDER BY created_at DESC LIMIT 50
│   │   ├── currency = "INR" AND amount >= 10000.00 INR AND created_at BETWEEN 2026-10-01@Asia/Kolkata AND 2026-11-01@Asia/Kolkata
│   │   ├── steps ANY (name = "capture_payment" AND status = "UNKNOWN") AND created_at >= now()-24h
│   │   ├── merchant_ref PREFIX "ACME-" AND tags HAS_ANY ("vip", "priority")
│   │   ├── NOT (status = "COMPLETED") AND completed_at IS NULL AND created_at < now()-2h      (stuck candidates)
│   │   └── description MATCH "\"late delivery\"" AND created_at >= now()-30d                  (SEARCH store)
│   │
│   └── 4.12 Examples (rejected, with error code)
│       ├── amount > 100                                   → TYPE_MISMATCH (money literal requires currency)
│       ├── amount > 100 USD                               → MONEY_CURRENCY_REQUIRED (no currency = "USD" predicate)
│       ├── steps.status = "FAILED"                        → NESTED_FIELD_OUTSIDE_QUANTIFIER
│       ├── created_at > "2026-11-01"                      → TIMEZONE_REQUIRED
│       ├── customer_email PREFIX "a"                      → OPERATOR_NOT_ALLOWED / FIELD_NOT_PERMITTED
│       ├── provider_reference = "x" (tenant user)         → FIELD_NOT_PERMITTED
│       ├── status = "PENDNG"                              → UNKNOWN_ENUM_VALUE (with suggestions)
│       ├── merchant_ref PREFIX "A"                        → PREFIX_TOO_SHORT
│       └── status = "FAILED" (huge tenant, no time bound) → TIME_RANGE_REQUIRED
│
├── 5. QUERY LIFECYCLE  (decision tree per request)
│   ├── Step 1 Authenticate caller (auth tree) → principal, tenant memberships, entitlements
│   ├── Step 2 Rate limit per principal and per tenant (cost-weighted tokens) → 429 + Retry-After
│   ├── Step 3 Parse (text → AST) or validate AST → INVALID_SYNTAX (line/column, expected tokens)
│   ├── Step 4 Validate against field registry for the requested TQL version
│   │   ├── UNKNOWN_FIELD / OPERATOR_NOT_ALLOWED / TYPE_MISMATCH / UNKNOWN_ENUM_VALUE /
│   │   │   NESTED_FIELD_OUTSIDE_QUANTIFIER / MONEY_CURRENCY_REQUIRED / TIMEZONE_REQUIRED
│   │   └── Deprecated field → allowed with warning header until removal version
│   ├── Step 5 Entitlement check on every referenced field (filter, sort, group, projection) (TQ-02)
│   │   └── Not entitled → FIELD_NOT_PERMITTED (same error whether field exists or not for unentitled callers)
│   ├── Step 6 Authorization injection (TQ-01)
│   │   ├── AND (tenant_id = caller tenant) — from verified membership, not from query
│   │   ├── AND (resource-relationship predicate) — e.g., owner/team/cost center per role
│   │   ├── AND (visibility rules) — e.g., hide internal-only transactions, archived per policy
│   │   └── Injected predicates are non-removable (applied outside the user's boolean tree)
│   ├── Step 7 Normalize (flatten, fold, resolve relative times once, bind params)
│   ├── Step 8 Cost estimation (TQ-04)
│   │   ├── Index coverage check for chosen store (all filter fields indexed or covered by a selective indexed predicate)
│   │   ├── Required time bound for large tenants
│   │   ├── Estimated rows scanned vs limit [G: thresholds]
│   │   └── Too expensive → QUERY_TOO_EXPENSIVE with hints (add time range, narrow status)
│   ├── Step 9 Plan & route (§2); compile to parameterized store query (TQ-03)
│   ├── Step 10 Execute with statement timeout and read-only transaction (PRIMARY) / request timeout (SEARCH)
│   │   ├── Timeout → QUERY_TIMEOUT (never partial results silently)
│   │   └── Store unavailable → fallback (if plan allows) or STORE_UNAVAILABLE (503)
│   ├── Step 11 Shape response: projection with masking; next cursor; consistency metadata (TQ-06)
│   ├── Step 12 Audit (sensitive queries, exports, operator/auditor access) (TQ-10)
│   └── Edge cases
│       ├── E5.1 Authorization applied as post-filter after LIMIT — pages short/empty; counts leak other tenants [C]
│       ├── E5.2 User's OR wraps injected tenant predicate ("tenant = A OR status = X") — cross-tenant leak [C]
│       │   └── Handling: injected predicates ANDed at root outside user tree (Step 6)
│       ├── E5.3 Error message differs for "field exists but forbidden" vs "unknown" — schema enumeration [C]
│       └── E5.4 Relative time resolved per page — pages from different windows — duplicates/missing items [C]
│           └── Handling: resolved times stored in cursor
│
├── 6. ENTITLEMENTS & DATA PROTECTION
│   ├── Entitlement classes mapped to roles (tenant user, finance, support, auditor, operator) [G]
│   ├── Masking at response (partial reveal rules per field, e.g., last 4 of reference)
│   ├── Oracle prevention: filtering/sorting/grouping on a field requires unmasked entitlement for it (TQ-02)
│   │   └── E6.1 Filter "customer_email = x@y.com" by unentitled user reveals existence via result count [C]
│   ├── Counts and aggregates computed only over the caller's authorized scope
│   ├── PII fields (encrypted at rest) searchable only via blind-index equality, never PREFIX/MATCH [C]
│   └── E6.2 Sorting by masked field leaks ordering information — sort requires entitlement [C]
│
├── 7. COST CONTROL & PLANNING  (TQ-04)
│   ├── Indexed field set per store published in registry; planner uses it
│   ├── Selectivity rules: at least one selective indexed predicate (id/ref equality, or time range) [L]
│   ├── Time partitions: queries pruned by created_at range on partitioned tables
│   ├── OR across different indexed fields → bounded number of index unions; beyond limit → reject
│   ├── NOT / != on low-selectivity fields → requires additional selective predicate
│   ├── Cost-weighted rate limiting (expensive queries consume more tokens)
│   └── Edge cases
│       ├── E7.1 status != "COMPLETED" on 100M rows — full scan — DB saturation — rejected pre-execution [C]
│       ├── E7.2 Hot tenant runs dashboards every 5 s — capacity drain — cost tokens throttle [C]
│       └── E7.3 Planner estimate wrong (stale stats) — timeout safety net + per-query circuit breaker [L]
│
├── 8. SORTING & PAGINATION  (TQ-05)
│   ├── Sort keys from sortable fields + mandatory unique tiebreaker (id)
│   ├── Keyset pagination: next page = rows after (sort key values, id) of last row
│   ├── Cursor contents (opaque, signed, expiring): query hash (normalized AST + injected predicates),
│   │   principal, tenant, sort keys + last values, resolved relative times, store, as-of watermark, TQL version
│   ├── Cursor validation
│   │   ├── Signature invalid / tampered → CURSOR_INVALID
│   │   ├── Expired [L: 15–60 min] → CURSOR_EXPIRED
│   │   ├── Different query/principal/tenant → CURSOR_QUERY_MISMATCH
│   │   └── Registry/TQL version changed incompatibly → CURSOR_INVALID (restart)
│   ├── Consistency modes
│   │   ├── LIVE (default): later pages reflect current data; items may move (documented)
│   │   └── SNAPSHOT (finance/exports): as-of watermark fixed; rows changed after watermark shown as-of [G: feasibility]
│   ├── Total counts: not returned by default; "count=estimate" or "count=exact" (exact only under cost limit)
│   └── Edge cases
│       ├── E8.1 Offset pagination at page 5,000 — deep scan — DB load [C]
│       ├── E8.2 Sort on non-unique field without tiebreaker — duplicates/missing across pages [C]
│       ├── E8.3 Item status changes between pages (LIVE) on status-filtered query — item disappears/appears [C]
│       ├── E8.4 Cursor shared with another user — rejected (principal bound) [C]
│       └── E8.5 Sorting by amount across currencies — meaningless order — rejected unless single currency [C]
│
├── 9. PROJECTIONS & RESPONSE
│   ├── fields=[...] selection from projectable fields (entitlement checked)
│   ├── Default projection per audience (minimal)
│   ├── Response metadata (TQ-06)
│   │   ├── store (PRIMARY|SEARCH), freshness (lag estimate), as_of watermark
│   │   ├── resolved relative times, tql_version, registry version
│   │   ├── warnings (deprecated fields, estimate counts)
│   │   └── next_cursor (absent on last page)
│   └── E9.1 Response omits store/freshness — user compares search result with detail page, sees mismatch, raises incident [C]
│
├── 10. AGGREGATIONS  (/transactions:aggregate)
│   ├── Functions: count, sum(amount), avg(amount), min/max(amount), count_distinct(field) (approximate flag)
│   ├── GROUP BY: groupable fields + time_bucket(field, size, zone)
│   ├── Money: sum/avg/min/max require currency in GROUP BY or single-currency filter (TQ-07)
│   ├── Limits: max groups [L: 1,000]; max buckets [L: 366 daily]; otherwise QUERY_TOO_EXPENSIVE
│   ├── Exactness: EXACT (PRIMARY, cost-limited) or APPROXIMATE (SEARCH, flagged)
│   ├── Finance use: period totals reconciled with reporting views, not with ad-hoc aggregates [G]
│   └── Edge cases
│       ├── E10.1 Sum across currencies — meaningless total — rejected [C]
│       ├── E10.2 Daily buckets in UTC for India users — revenue shifts across days — zone mandatory [C]
│       └── E10.3 Approximate distinct counts presented as exact — wrong business decisions — flagged in response [C]
│
├── 11. CONSISTENCY & FRESHNESS
│   ├── Default: eventual (materialized views); freshness reported
│   ├── Read-your-writes: requests may carry the signed consistency token from command responses (CQRS tree)
│   │   ├── GET by id: wait (bounded) for version ≥ token, else PENDING status
│   │   └── Search: if routed store watermark < token → response flagged INCOMPLETE + pending item hint
│   ├── PRIMARY vs SEARCH differences reported (store field)
│   └── E11.1 User creates transaction, searches immediately, doesn't see it — flagged INCOMPLETE, not silently missing [C]
│
├── 12. GET-BY-ID & TIMELINE
│   ├── GET by id
│   │   ├── Authorization (tenant + relationship) → not visible → 404 (indistinguishable from missing)
│   │   ├── Valid consistency token and not yet projected → 200 with status PENDING (never 404)
│   │   └── Masked fields per entitlement
│   ├── Timeline
│   │   ├── Steps, attempts, outcomes (business wording), compensations, timestamps (with zone info)
│   │   ├── Internal details (error fingerprints, idempotency fingerprints, worker builds) operator-only
│   │   └── Large histories paginated (cursor) [L]
│   └── E12.1 Timeline exposes provider error messages with other customers' data — leak — sanitized messages only [C]
│
├── 13. EXPORTS & SAVED QUERIES
│   ├── Export
│   │   ├── Async job: TQL validated/authorized/costed like search; SNAPSHOT consistency
│   │   ├── Row cap per export [G]; chunked generation; file encrypted; download link short-lived and principal-bound
│   │   ├── Re-authorization at download time (entitlement may have changed)
│   │   └── Audit: who, what query, row count, downloaded by whom/when
│   ├── Saved queries
│   │   ├── Stored as normalized AST + TQL version; owner; visibility (private / team within tenant)
│   │   ├── Executed with the RUNNER's authorization (never owner's)
│   │   └── Registry changes → saved query revalidated; broken ones flagged to owner
│   └── Edge cases
│       ├── E13.1 Shared saved query runs with creator's broader rights — privilege escalation [C]
│       └── E13.2 Export link forwarded to someone else — principal-bound link refuses [C]
│
├── 14. ERROR CATALOG  (typed; TQ-09)
│   ├── 400 INVALID_SYNTAX (position, expected) | UNKNOWN_FIELD | OPERATOR_NOT_ALLOWED | TYPE_MISMATCH |
│   │       UNKNOWN_ENUM_VALUE | NESTED_FIELD_OUTSIDE_QUANTIFIER | MONEY_CURRENCY_REQUIRED | TIMEZONE_REQUIRED |
│   │       PREFIX_TOO_SHORT | LIMIT_EXCEEDED | CURSOR_INVALID | CURSOR_QUERY_MISMATCH
│   ├── 403 FIELD_NOT_PERMITTED (also returned for unknown fields to unentitled callers where enumeration matters)
│   ├── 404 NOT_FOUND (single resource; not visible or nonexistent)
│   ├── 410 CURSOR_EXPIRED
│   ├── 422 QUERY_TOO_EXPENSIVE (with hints) | TIME_RANGE_REQUIRED | RESULT_WINDOW_EXCEEDED
│   ├── 429 RATE_LIMITED (Retry-After, cost info)
│   ├── 503 STORE_UNAVAILABLE (no partial results)
│   └── 504 QUERY_TIMEOUT
│
├── 15. LANGUAGE & FIELD VERSIONING  (TQ-11)
│   ├── tql_version pinned per request; registry versioned; new fields additive
│   ├── Field removal: deprecate (warning) → remove in next version; saved queries flagged
│   ├── Semantics changes (e.g., status mapping) → new version only
│   └── E15.1 Status enum renamed in place — saved queries and integrations silently return empty results [C]
│
├── 16. QUERY AUDIT  (TQ-10)
│   ├── Audited: operator/auditor queries, queries touching FINANCIAL/PII/RESTRICTED fields, exports, impersonated access
│   ├── Record: principal, tenant, normalized AST (values of PII predicates hashed), result count, purpose/case id
│   └── E16.1 Audit stores raw PII filter values — audit trail becomes PII store [C]
│
└── 17. FEATURE TEST SCENARIOS
    ├── Tenant A user query with "OR status = X" never returns tenant B rows (root-level injection)
    ├── Relationship rule: user sees only own/assigned transactions; tenant-wide role sees all in tenant
    ├── Filter on unentitled field → FIELD_NOT_PERMITTED; sort/group on it also rejected
    ├── steps ANY (name = payment AND status = FAILED) matches only same-step combinations
    ├── amount filter without currency → rejected; with currency → correct minor-unit comparison
    ├── Date range in Asia/Kolkata → correct UTC bounds; DST zone ranges correct
    ├── status != COMPLETED on huge tenant without time bound → TIME_RANGE_REQUIRED before execution
    ├── Keyset pagination across 10,000 rows → no duplicates/missing with stable sort + tiebreaker
    ├── Tampered/foreign/expired cursor → correct error codes
    ├── Relative time "now()-7d" consistent across all pages of one query
    ├── Search store lagging → response marks SEARCH + freshness; newly created item flagged INCOMPLETE with token
    ├── Aggregate sum without currency grouping → rejected
    ├── Shared saved query executes with runner's rights
    ├── Export link used by another principal → refused
    ├── Injection attempt in string literal → treated as value (parameterized), no effect
    ├── Store timeout → 504, no partial page
    └── Operator query on cross-tenant data → requires impersonation session, audited
```

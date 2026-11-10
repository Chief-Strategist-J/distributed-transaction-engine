package contracts

import (
	"time"

	"github.com/google/uuid"
)

const (
	TQLVersionV1         = "1"
	RegistryVersionV1    = "2026-11-01"
	DefaultPageSize      = 50
	MaxPageSize          = 200
	MaxQueryTextBytes    = 4096
	MaxASTNodes          = 100
	MaxBooleanDepth      = 8
	MaxInListSize        = 500
	MaxOrBranches        = 20
	MaxSortKeys          = 3
	MaxStringLiteral     = 256
	MinPrefixLength      = 3
	MaxParams            = 50
	MaxProjectedFields   = 40
	MaxTeamIDs           = 50
	MaxGroupBy           = 3
	MaxAggregateGroups   = 1000
	MaxTimeBuckets       = 366
	MaxExportRows        = 100000
	ExportChunkRows      = 1000
	MaxSavedQueryNameLen = 128
	MaxPurposeLength     = 256
)

type StoreName string

const (
	StorePrimary StoreName = "PRIMARY"
	StoreSearch  StoreName = "SEARCH"
	StoreHistory StoreName = "HISTORY"
)

type FieldType string

const (
	FieldTypeOpaqueID  FieldType = "OPAQUE_ID"
	FieldTypeEnum      FieldType = "ENUM"
	FieldTypeReference FieldType = "REFERENCE"
	FieldTypeTimestamp FieldType = "TIMESTAMP"
	FieldTypeMoney     FieldType = "MONEY"
	FieldTypeInteger   FieldType = "INTEGER"
	FieldTypeBoolean   FieldType = "BOOLEAN"
	FieldTypeSet       FieldType = "SET"
	FieldTypeText      FieldType = "TEXT"
	FieldTypeSensitive FieldType = "SENSITIVE_STRING"
)

type Operator string

const (
	OpEq        Operator = "="
	OpNe        Operator = "!="
	OpLt        Operator = "<"
	OpLe        Operator = "<="
	OpGt        Operator = ">"
	OpGe        Operator = ">="
	OpPrefix    Operator = "PREFIX"
	OpIn        Operator = "IN"
	OpNotIn     Operator = "NOT IN"
	OpBetween   Operator = "BETWEEN"
	OpIsNull    Operator = "IS NULL"
	OpIsNotNull Operator = "IS NOT NULL"
	OpMatch     Operator = "MATCH"
	OpHas       Operator = "HAS"
	OpHasAny    Operator = "HAS_ANY"
	OpHasAll    Operator = "HAS_ALL"
)

type EntitlementClass string

const (
	EntitlementPublic     EntitlementClass = "PUBLIC"
	EntitlementInternal   EntitlementClass = "INTERNAL"
	EntitlementFinancial  EntitlementClass = "FINANCIAL"
	EntitlementPII        EntitlementClass = "PII"
	EntitlementRestricted EntitlementClass = "RESTRICTED"
)

type MaskRule string

const (
	MaskNone         MaskRule = "NONE"
	MaskLastFour     MaskRule = "LAST_FOUR"
	MaskEmailPartial MaskRule = "EMAIL_PARTIAL"
)

type Audience string

const (
	AudienceTenantUser      Audience = "TENANT_USER"
	AudienceTenantAPIClient Audience = "TENANT_API_CLIENT"
	AudienceOperator        Audience = "OPERATOR"
	AudienceFinanceAuditor  Audience = "FINANCE_AUDITOR"
	AudienceInternalService Audience = "INTERNAL_SERVICE"
)

type RelationshipScope string

const (
	ScopeTenantWide      RelationshipScope = "TENANT_WIDE"
	ScopeOwnedOrAssigned RelationshipScope = "OWNED_OR_ASSIGNED"
)

type NodeKind string

const (
	NodeAnd        NodeKind = "AND"
	NodeOr         NodeKind = "OR"
	NodeNot        NodeKind = "NOT"
	NodePredicate  NodeKind = "PREDICATE"
	NodeQuantified NodeKind = "QUANTIFIED"
)

type Quantifier string

const (
	QuantifierAny  Quantifier = "ANY"
	QuantifierAll  Quantifier = "ALL"
	QuantifierNone Quantifier = "NONE"
)

type ValueKind string

const (
	ValueString   ValueKind = "STRING"
	ValueNumber   ValueKind = "NUMBER"
	ValueBoolean  ValueKind = "BOOLEAN"
	ValueMoney    ValueKind = "MONEY"
	ValueTime     ValueKind = "TIMESTAMP"
	ValueRelative ValueKind = "RELATIVE_TIME"
	ValueParam    ValueKind = "PARAM"
)

type RelativeBase string

const (
	RelativeNow     RelativeBase = "NOW"
	RelativeStartOf RelativeBase = "START_OF"
)

type RelativeTime struct {
	Base    RelativeBase `json:"base"`
	Sign    int          `json:"sign,omitempty"`
	Amount  int          `json:"amount,omitempty"`
	Unit    string       `json:"unit,omitempty"`
	StartOf string       `json:"start_of,omitempty"`
	Zone    string       `json:"zone,omitempty"`
}

type Value struct {
	Kind     ValueKind     `json:"kind"`
	Text     string        `json:"text,omitempty"`
	Currency string        `json:"currency,omitempty"`
	Zone     string        `json:"zone,omitempty"`
	Relative *RelativeTime `json:"relative,omitempty"`
}

type Predicate struct {
	Field    string   `json:"field"`
	Operator Operator `json:"operator"`
	Values   []Value  `json:"values,omitempty"`
}

type QuantifiedNode struct {
	Collection string     `json:"collection"`
	Quantifier Quantifier `json:"quantifier"`
	Body       *Node      `json:"body"`
}

type Node struct {
	Kind       NodeKind        `json:"kind"`
	Children   []Node          `json:"children,omitempty"`
	Predicate  *Predicate      `json:"predicate,omitempty"`
	Quantified *QuantifiedNode `json:"quantified,omitempty"`
}

type NullsOrder string

const (
	NullsDefault NullsOrder = ""
	NullsFirst   NullsOrder = "FIRST"
	NullsLast    NullsOrder = "LAST"
)

type SortKey struct {
	Field      string     `json:"field"`
	Descending bool       `json:"descending,omitempty"`
	Nulls      NullsOrder `json:"nulls,omitempty"`
}

type Query struct {
	Filter  *Node     `json:"filter,omitempty"`
	OrderBy []SortKey `json:"order_by,omitempty"`
	Limit   int       `json:"limit,omitempty"`
}

type CountMode string

const (
	CountNone     CountMode = "NONE"
	CountEstimate CountMode = "ESTIMATE"
	CountExact    CountMode = "EXACT"
)

type ConsistencyMode string

const (
	ConsistencyLive     ConsistencyMode = "LIVE"
	ConsistencySnapshot ConsistencyMode = "SNAPSHOT"
)

type Exactness string

const (
	ExactnessExact       Exactness = "EXACT"
	ExactnessApproximate Exactness = "APPROXIMATE"
)

type Completeness string

const (
	CompletenessComplete   Completeness = "COMPLETE"
	CompletenessIncomplete Completeness = "INCOMPLETE"
)

type FieldDefinition struct {
	Name           string
	Type           FieldType
	Nullable       bool
	Operators      []Operator
	Filterable     bool
	Sortable       bool
	Groupable      bool
	Searchable     bool
	Projectable    bool
	IndexedIn      []StoreName
	Entitlement    EntitlementClass
	OperatorOnly   bool
	Mask           MaskRule
	Deprecated     bool
	RemovalVersion string
	SinceVersion   string
	Collection     string
	MaxLength      int
	EnumValues     []string
	OperatorEnum   []string
}

type CollectionDefinition struct {
	Name   string
	Fields []string
}

type Registry struct {
	TQLVersion      string
	RegistryVersion string
	Fields          map[string]FieldDefinition
	Collections     map[string]CollectionDefinition
}

type ImpersonationSession struct {
	OperatorID  string    `json:"operator_id"`
	TenantScope []string  `json:"tenant_scope"`
	Reason      string    `json:"reason"`
	TicketID    string    `json:"ticket_id"`
	CaseID      string    `json:"case_id"`
	IssuedAt    time.Time `json:"issued_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type Principal struct {
	PrincipalID       string
	TenantID          string
	Audience          Audience
	Entitlements      []EntitlementClass
	RelationshipScope RelationshipScope
	TeamIDs           []string
	Impersonation     *ImpersonationSession
}

type AuthenticatedCaller struct {
	CallerType    string
	PrincipalID   string
	TenantID      string
	Roles         []string
	Scopes        []string
	TeamIDs       []string
	Impersonation *ImpersonationSession
}

type SearchRequest struct {
	TQLVersion       string            `json:"tql_version"`
	Text             *string           `json:"text,omitempty"`
	AST              *Query            `json:"ast,omitempty"`
	Params           map[string]string `json:"params,omitempty"`
	Fields           []string          `json:"fields,omitempty"`
	Cursor           *string           `json:"cursor,omitempty"`
	Count            CountMode         `json:"count,omitempty"`
	Consistency      ConsistencyMode   `json:"consistency,omitempty"`
	ConsistencyToken *string           `json:"consistency_token,omitempty"`
	TenantID         *string           `json:"tenant_id,omitempty"`
	Purpose          *string           `json:"purpose,omitempty"`
}

type AggregateFunction string

const (
	AggCount         AggregateFunction = "COUNT"
	AggSum           AggregateFunction = "SUM"
	AggAvg           AggregateFunction = "AVG"
	AggMin           AggregateFunction = "MIN"
	AggMax           AggregateFunction = "MAX"
	AggCountDistinct AggregateFunction = "COUNT_DISTINCT"
)

type AggregateExpression struct {
	Function AggregateFunction `json:"function"`
	Field    string            `json:"field,omitempty"`
}

type TimeBucket struct {
	Field string `json:"field"`
	Size  string `json:"size"`
	Zone  string `json:"zone"`
}

type AggregateRequest struct {
	TQLVersion string                `json:"tql_version"`
	Text       *string               `json:"text,omitempty"`
	AST        *Query                `json:"ast,omitempty"`
	Params     map[string]string     `json:"params,omitempty"`
	Aggregates []AggregateExpression `json:"aggregates"`
	GroupBy    []string              `json:"group_by,omitempty"`
	TimeBucket *TimeBucket           `json:"time_bucket,omitempty"`
	Exactness  Exactness             `json:"exactness,omitempty"`
	TenantID   *string               `json:"tenant_id,omitempty"`
	Purpose    *string               `json:"purpose,omitempty"`
}

type ResolvedTime struct {
	Expression string    `json:"expression"`
	Instant    time.Time `json:"instant"`
}

type CountResult struct {
	Mode  CountMode `json:"mode"`
	Value int64     `json:"value"`
}

type ResponseMeta struct {
	Store               StoreName       `json:"store"`
	FreshnessLagSeconds int64           `json:"freshness_lag_seconds"`
	AsOf                time.Time       `json:"as_of"`
	AsOfSequence        int64           `json:"as_of_sequence"`
	Completeness        Completeness    `json:"completeness"`
	PendingHints        []string        `json:"pending_hints,omitempty"`
	ResolvedTimes       []ResolvedTime  `json:"resolved_times,omitempty"`
	TQLVersion          string          `json:"tql_version"`
	RegistryVersion     string          `json:"registry_version"`
	Warnings            []string        `json:"warnings,omitempty"`
	Count               *CountResult    `json:"count,omitempty"`
	NextCursor          *string         `json:"next_cursor,omitempty"`
	Consistency         ConsistencyMode `json:"consistency"`
	Exactness           Exactness       `json:"exactness,omitempty"`
	ResultsEmptyByProof bool            `json:"results_empty_by_proof,omitempty"`
}

type Row map[string]any

type SearchResponse struct {
	Items []Row        `json:"items"`
	Meta  ResponseMeta `json:"meta"`
}

type AggregateGroup struct {
	Keys    map[string]any `json:"keys"`
	Metrics map[string]any `json:"metrics"`
}

type AggregateResponse struct {
	Groups []AggregateGroup `json:"groups"`
	Meta   ResponseMeta     `json:"meta"`
}

type TransactionDetail struct {
	Item    Row          `json:"item"`
	Pending bool         `json:"pending"`
	Meta    ResponseMeta `json:"meta"`
}

type TimelineEvent struct {
	StepName       string     `json:"step_name"`
	State          string     `json:"state"`
	BusinessState  string     `json:"business_state"`
	Attempts       int        `json:"attempts"`
	Message        string     `json:"message,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	TimeZone       string     `json:"time_zone"`
	IsCompensation bool       `json:"is_compensation"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
}

type TimelineRequest struct {
	TenantID      *string `json:"tenant_id,omitempty"`
	TransactionID string  `json:"transaction_id"`
	Cursor        *string `json:"cursor,omitempty"`
	Limit         int     `json:"limit,omitempty"`
}

type TimelineResponse struct {
	Events     []TimelineEvent `json:"events"`
	NextCursor *string         `json:"next_cursor,omitempty"`
	Meta       ResponseMeta    `json:"meta"`
}

type GetTransactionRequest struct {
	TenantID         *string  `json:"tenant_id,omitempty"`
	TransactionID    string   `json:"transaction_id"`
	Fields           []string `json:"fields,omitempty"`
	ConsistencyToken *string  `json:"consistency_token,omitempty"`
	TQLVersion       string   `json:"tql_version"`
}

type ExportStatus string

const (
	ExportQueued    ExportStatus = "QUEUED"
	ExportRunning   ExportStatus = "RUNNING"
	ExportCompleted ExportStatus = "COMPLETED"
	ExportFailed    ExportStatus = "FAILED"
	ExportExpired   ExportStatus = "EXPIRED"
	ExportCancelled ExportStatus = "CANCELLED"
)

type ExportRequest struct {
	TQLVersion string            `json:"tql_version"`
	Text       *string           `json:"text,omitempty"`
	AST        *Query            `json:"ast,omitempty"`
	Params     map[string]string `json:"params,omitempty"`
	Fields     []string          `json:"fields,omitempty"`
	TenantID   *string           `json:"tenant_id,omitempty"`
	Purpose    *string           `json:"purpose,omitempty"`
}

type ExportJob struct {
	ID            uuid.UUID    `json:"-"`
	PublicID      string       `json:"id"`
	TenantID      string       `json:"-"`
	PrincipalID   string       `json:"-"`
	Status        ExportStatus `json:"status"`
	QueryJSON     []byte       `json:"-"`
	Fields        []string     `json:"fields"`
	TQLVersion    string       `json:"tql_version"`
	AsOfSequence  int64        `json:"as_of_sequence"`
	RowCount      int64        `json:"row_count"`
	FailureCode   *string      `json:"failure_code,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
	CompletedAt   *time.Time   `json:"completed_at,omitempty"`
	ExpiresAt     time.Time    `json:"expires_at"`
	Version       int64        `json:"-"`
	PrincipalJSON []byte       `json:"-"`
	CorrelationID uuid.UUID    `json:"-"`
}

type ExportLink struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type ExportDownload struct {
	ContentType string `json:"content_type"`
	FileName    string `json:"file_name"`
	Content     []byte `json:"-"`
	RowCount    int64  `json:"row_count"`
}

type SavedQueryVisibility string

const (
	VisibilityPrivate SavedQueryVisibility = "PRIVATE"
	VisibilityTeam    SavedQueryVisibility = "TEAM"
)

type SavedQueryStatus string

const (
	SavedQueryValid  SavedQueryStatus = "VALID"
	SavedQueryBroken SavedQueryStatus = "BROKEN"
)

type SavedQuery struct {
	ID              uuid.UUID            `json:"-"`
	PublicID        string               `json:"id"`
	TenantID        string               `json:"-"`
	OwnerID         string               `json:"owner_id"`
	Visibility      SavedQueryVisibility `json:"visibility"`
	TeamRef         *string              `json:"team_ref,omitempty"`
	Name            string               `json:"name"`
	TQLVersion      string               `json:"tql_version"`
	RegistryVersion string               `json:"registry_version"`
	QueryJSON       []byte               `json:"-"`
	Status          SavedQueryStatus     `json:"status"`
	BrokenReason    *string              `json:"broken_reason,omitempty"`
	Version         int64                `json:"version"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
}

type CreateSavedQueryRequest struct {
	Name       string               `json:"name"`
	Visibility SavedQueryVisibility `json:"visibility"`
	TeamRef    *string              `json:"team_ref,omitempty"`
	TQLVersion string               `json:"tql_version"`
	Text       *string              `json:"text,omitempty"`
	AST        *Query               `json:"ast,omitempty"`
	Params     map[string]string    `json:"params,omitempty"`
}

type RunSavedQueryRequest struct {
	SavedQueryID string    `json:"saved_query_id"`
	Cursor       *string   `json:"cursor,omitempty"`
	Count        CountMode `json:"count,omitempty"`
}

type AuditAction string

const (
	AuditSearch         AuditAction = "SEARCH"
	AuditAggregate      AuditAction = "AGGREGATE"
	AuditGet            AuditAction = "GET"
	AuditTimeline       AuditAction = "TIMELINE"
	AuditExportCreate   AuditAction = "EXPORT_CREATE"
	AuditExportDownload AuditAction = "EXPORT_DOWNLOAD"
	AuditSavedQueryRun  AuditAction = "SAVED_QUERY_RUN"
)

type AuditRecord struct {
	ID             uuid.UUID
	PublicID       string
	TenantID       string
	PrincipalID    string
	Audience       Audience
	Action         AuditAction
	NormalizedAST  []byte
	ResultCount    *int64
	Purpose        *string
	ClassesTouched []string
	Impersonated   bool
	CorrelationID  uuid.UUID
	OccurredAt     time.Time
}

type ProjectionRecord struct {
	TenantID           string
	PublicID           string
	SagaID             uuid.UUID
	InternalStatus     string
	BusinessStatus     string
	WorkflowType       string
	WorkflowVersion    string
	CreatedAt          time.Time
	EffectiveAt        time.Time
	CompletedAt        *time.Time
	AmountMinor        int64
	Currency           string
	CustomerRef        string
	CustomerEmailBlind *string
	CustomerEmailMask  *string
	MerchantRef        *string
	ProviderReference  *string
	CorrelationID      uuid.UUID
	Tags               []string
	Description        *string
	CompensationStatus string
	NeedsAttention     bool
	OwnerRef           string
	TeamRef            *string
	IsInternalOnly     bool
	ArchivedAt         *time.Time
	Steps              []ProjectionStep
}

type ProjectionStep struct {
	Name           string
	Status         string
	Attempts       int
	LastErrorClass *string
	CompletedAt    *time.Time
}

type Watermark struct {
	Store      StoreName
	Sequence   int64
	AsOf       time.Time
	Rebuilding bool
}

type ConsistencyClaims struct {
	TenantID      string `json:"t"`
	TransactionID string `json:"x,omitempty"`
	MinSequence   int64  `json:"s"`
	ExpiresAt     int64  `json:"e"`
}

type CursorValue struct {
	Null bool   `json:"n,omitempty"`
	Text string `json:"v,omitempty"`
}

type CursorClaims struct {
	TQLVersion      string          `json:"v"`
	RegistryVersion string          `json:"r"`
	QueryHash       string          `json:"q"`
	PrincipalID     string          `json:"p"`
	TenantID        string          `json:"t"`
	SortFields      []string        `json:"k"`
	LastValues      []CursorValue   `json:"l"`
	ResolvedNow     time.Time       `json:"n"`
	Store           StoreName       `json:"s"`
	AsOfSequence    int64           `json:"a"`
	Consistency     ConsistencyMode `json:"c"`
	ExpiresAt       int64           `json:"e"`
}

type ExportLinkClaims struct {
	JobID       string `json:"j"`
	PrincipalID string `json:"p"`
	TenantID    string `json:"t"`
	ExpiresAt   int64  `json:"e"`
}

type FieldUsageKind string

const (
	UsageFilter    FieldUsageKind = "FILTER"
	UsageSort      FieldUsageKind = "SORT"
	UsageGroup     FieldUsageKind = "GROUP"
	UsageProject   FieldUsageKind = "PROJECT"
	UsageMatch     FieldUsageKind = "MATCH"
	UsageAggregate FieldUsageKind = "AGGREGATE"
)

type FieldUse struct {
	Field string
	Usage FieldUsageKind
}

type InjectedPredicates struct {
	TenantID            string
	RestrictToOwner     bool
	OwnerRef            string
	TeamRefs            []string
	ExcludeInternalOnly bool
	ExcludeArchived     bool
}

type NormalizedQuery struct {
	Filter        Node
	OrderBy       []SortKey
	Limit         int
	Uses          []FieldUse
	ResolvedTimes []ResolvedTime
	Warnings      []string
	Contradiction bool
	HasMatch      bool
}

type KeysetPosition struct {
	Keys []SortKey
	Last []CursorValue
}

type QueryPlan struct {
	Store        StoreName
	Injected     InjectedPredicates
	Filter       Node
	OrderBy      []SortKey
	Limit        int
	Keyset       *KeysetPosition
	Fields       []string
	AsOfSequence *int64
}

type AggregatePlan struct {
	Store        StoreName
	Injected     InjectedPredicates
	Filter       Node
	Aggregates   []AggregateExpression
	GroupBy      []string
	TimeBucket   *TimeBucket
	AsOfSequence *int64
}

type CostInput struct {
	Store             StoreName
	Query             NormalizedQuery
	TenantRowEstimate int64
	Now               time.Time
}

type CostDecision struct {
	Selective bool
	TimeBound bool
	Tokens    int
}

type PrepareInput struct {
	TQLVersion           string
	Text                 *string
	AST                  *Query
	Params               map[string]string
	Fields               []string
	Cursor               *string
	Consistency          ConsistencyMode
	ConsistencyToken     *string
	TenantID             *string
	Purpose              *string
	SkipRateLimit        bool
	ForbidOrderAndLimit  bool
	AsOfSequenceOverride *int64
}

type PreparedQuery struct {
	Registry          Registry
	Principal         Principal
	TenantID          string
	Normalized        NormalizedQuery
	QueryHash         string
	Store             StoreName
	Injected          InjectedPredicates
	Fields            []string
	FieldUses         []FieldUse
	CursorClaims      *CursorClaims
	ConsistencyClaims *ConsistencyClaims
	Consistency       ConsistencyMode
	AsOfSequence      *int64
	ResolvedNow       time.Time
	Purpose           *string
	CostTokens        int
	TenantRowEstimate int64
	Unresolved        Query
	SourceParams      map[string]string
	TQLVersion        string
}

type PreparedAggregate struct {
	Prepared  PreparedQuery
	Request   AggregateRequest
	Exactness Exactness
}

type TimelineRow struct {
	Event      TimelineEvent
	CreatedAt  time.Time
	StepID     string
	RawMessage string
}

type TimelinePosition struct {
	CreatedAt time.Time
	StepID    string
}

type RateDecision struct {
	Allowed           bool
	RetryAfterSeconds int
}

type ExportChunk struct {
	JobID      uuid.UUID
	Sequence   int
	Ciphertext []byte
	RowCount   int
}

func (q Query) FilterOrEmpty() Node {
	if q.Filter == nil {
		return Node{Kind: NodeAnd}
	}
	return *q.Filter
}

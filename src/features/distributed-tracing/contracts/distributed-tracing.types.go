package contracts

import (
	"time"

	"github.com/google/uuid"
)

type SpanKind string

const (
	SpanKindInternal SpanKind = "INTERNAL"
	SpanKindServer   SpanKind = "SERVER"
	SpanKindClient   SpanKind = "CLIENT"
	SpanKindProducer SpanKind = "PRODUCER"
	SpanKindConsumer SpanKind = "CONSUMER"
)

type SpanStatusCode string

const (
	SpanStatusUnset SpanStatusCode = "UNSET"
	SpanStatusOK    SpanStatusCode = "OK"
	SpanStatusError SpanStatusCode = "ERROR"
)

type DeepTargetType string

const (
	DeepTargetSagaID        DeepTargetType = "SAGA_ID"
	DeepTargetCorrelationID DeepTargetType = "CORRELATION_ID"
	DeepTargetTenantID      DeepTargetType = "TENANT_ID"
	DeepTargetRoute         DeepTargetType = "ROUTE"
)

type DeepSessionStatus string

const (
	DeepSessionActive    DeepSessionStatus = "ACTIVE"
	DeepSessionExpired   DeepSessionStatus = "EXPIRED"
	DeepSessionCancelled DeepSessionStatus = "CANCELLED"
	DeepSessionCapped    DeepSessionStatus = "CAPPED"
)

type AsyncLinkType string

const (
	AsyncLinkDispatch     AsyncLinkType = "DISPATCH"
	AsyncLinkReply        AsyncLinkType = "REPLY"
	AsyncLinkTimer        AsyncLinkType = "TIMER"
	AsyncLinkCompensation AsyncLinkType = "COMPENSATION"
	AsyncLinkIntervention AsyncLinkType = "INTERVENTION"
)

type TraceContext struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	TraceFlags byte              `json:"trace_flags"`
	TraceState string            `json:"trace_state,omitempty"`
	Baggage    map[string]string `json:"baggage,omitempty"`
	IsSampled  bool              `json:"is_sampled"`
	IsDeepMode bool              `json:"is_deep_mode"`
}

type SpanEvent struct {
	Name       string                 `json:"name"`
	Timestamp  time.Time              `json:"timestamp"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

type SpanLink struct {
	TraceID    string                 `json:"trace_id"`
	SpanID     string                 `json:"span_id"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

type RecordSpanCommand struct {
	TenantID     string                 `json:"tenant_id"`
	TraceID      string                 `json:"trace_id"`
	SpanID       string                 `json:"span_id"`
	ParentSpanID string                 `json:"parent_span_id,omitempty"`
	SpanName     string                 `json:"span_name"`
	SpanKind     SpanKind               `json:"span_kind"`
	StartTime    time.Time              `json:"start_time"`
	EndTime      time.Time              `json:"end_time"`
	StatusCode   SpanStatusCode         `json:"status_code"`
	Attributes   map[string]interface{} `json:"attributes,omitempty"`
	Events       []SpanEvent            `json:"events,omitempty"`
	Links        []SpanLink             `json:"links,omitempty"`
}

type RegisterAsyncLinkCommand struct {
	TenantID    string        `json:"tenant_id"`
	SagaID      uuid.UUID     `json:"saga_id"`
	StepName    string        `json:"step_name"`
	Traceparent string        `json:"traceparent"`
	Tracestate  string        `json:"tracestate,omitempty"`
	LinkType    AsyncLinkType `json:"link_type"`
}

type ResolveAsyncLinkQuery struct {
	TenantID string        `json:"tenant_id"`
	SagaID   uuid.UUID     `json:"saga_id"`
	StepName string        `json:"step_name"`
	LinkType AsyncLinkType `json:"link_type"`
}

type ActivateDeepSessionCommand struct {
	TenantID     string         `json:"tenant_id"`
	TargetType   DeepTargetType `json:"target_type"`
	TargetValue  string         `json:"target_value"`
	SamplingRate float64        `json:"sampling_rate"`
	MaxSpans     int            `json:"max_spans"`
	DurationSec  int            `json:"duration_sec"`
}

type EvaluateSamplingQuery struct {
	TenantID       string    `json:"tenant_id"`
	CorrelationID  uuid.UUID `json:"correlation_id"`
	IsVIP          bool      `json:"is_vip"`
	IsError        bool      `json:"is_error"`
	IsCompensation bool      `json:"is_compensation"`
}

type SpanRecord struct {
	ID            uuid.UUID              `json:"id"`
	PublicID      string                 `json:"public_id"`
	TenantID      string                 `json:"tenant_id"`
	Source        string                 `json:"source"`
	ExternalID    string                 `json:"external_id"`
	CorrelationID uuid.UUID              `json:"correlation_id"`
	TraceID       string                 `json:"trace_id"`
	SpanID        string                 `json:"span_id"`
	ParentSpanID  string                 `json:"parent_span_id"`
	SpanName      string                 `json:"span_name"`
	SpanKind      SpanKind               `json:"span_kind"`
	StartTime     time.Time              `json:"start_time"`
	EndTime       time.Time              `json:"end_time"`
	DurationNS    int64                  `json:"duration_ns"`
	StatusCode    SpanStatusCode         `json:"status_code"`
	Attributes    map[string]interface{} `json:"attributes"`
	Events        []SpanEvent            `json:"events"`
	Links         []SpanLink             `json:"links"`
	CreatedAt     time.Time              `json:"created_at"`
}

type DeepSessionRecord struct {
	ID            uuid.UUID         `json:"id"`
	PublicID      string            `json:"public_id"`
	TenantID      string            `json:"tenant_id"`
	Source        string            `json:"source"`
	ExternalID    string            `json:"external_id"`
	CorrelationID uuid.UUID         `json:"correlation_id"`
	TargetType    DeepTargetType    `json:"target_type"`
	TargetValue   string            `json:"target_value"`
	SamplingRate  float64           `json:"sampling_rate"`
	MaxSpans      int               `json:"max_spans"`
	ExpiresAt     time.Time         `json:"expires_at"`
	Status        DeepSessionStatus `json:"status"`
	CreatedAt     time.Time         `json:"created_at"`
}

type SamplingRuleRecord struct {
	ID            uuid.UUID              `json:"id"`
	PublicID      string                 `json:"public_id"`
	TenantID      string                 `json:"tenant_id"`
	Source        string                 `json:"source"`
	ExternalID    string                 `json:"external_id"`
	CorrelationID uuid.UUID              `json:"correlation_id"`
	RuleName      string                 `json:"rule_name"`
	MatchCriteria map[string]interface{} `json:"match_criteria"`
	SampleRate    float64                `json:"sample_rate"`
	IsVIP         bool                   `json:"is_vip"`
	Enabled       bool                   `json:"enabled"`
	CreatedAt     time.Time              `json:"created_at"`
}

type AsyncLinkRecord struct {
	ID            uuid.UUID     `json:"id"`
	PublicID      string        `json:"public_id"`
	TenantID      string        `json:"tenant_id"`
	Source        string        `json:"source"`
	ExternalID    string        `json:"external_id"`
	CorrelationID uuid.UUID     `json:"correlation_id"`
	SagaID        uuid.UUID     `json:"saga_id"`
	StepName      string        `json:"step_name"`
	Traceparent   string        `json:"traceparent"`
	Tracestate    string        `json:"tracestate"`
	LinkType      AsyncLinkType `json:"link_type"`
	CreatedAt     time.Time     `json:"created_at"`
}


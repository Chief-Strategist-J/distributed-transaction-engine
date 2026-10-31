package types

type TraceContext struct {
	TraceID      string `json:"traceId"`
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
}

type RequestContext struct {
	TraceContext   TraceContext `json:"traceContext"`
	RequestID      string       `json:"requestId"`
	CorrelationID  string       `json:"correlationId"`
	CausationID    string       `json:"causationId"`
	IdempotencyKey string       `json:"idempotencyKey"`
	TenantID       string       `json:"tenantId"`
	ClientID       string       `json:"clientId"`
	UserID         string       `json:"userId"`
}

type ResponseMeta struct {
	RequestID       string `json:"requestId"`
	CorrelationID   string `json:"correlationId"`
	CausationID     string `json:"causationId"`
	Timestamp       string `json:"timestamp"`
	ExecutionTimeMs int64  `json:"executionTimeMs"`
	APIVersion      string `json:"apiVersion"`
}

type ResponseErrorDetail struct {
	Field         string      `json:"field"`
	Issue         string      `json:"issue"`
	Rule          string      `json:"rule"`
	RejectedValue interface{} `json:"rejectedValue,omitempty"`
}

type ResponseError struct {
	Code      string                `json:"code"`
	Message   string                `json:"message"`
	Retryable bool                  `json:"retryable"`
	Details   []ResponseErrorDetail `json:"details,omitempty"`
}

type ApiResponse struct {
	Success    bool           `json:"success"`
	StatusCode int            `json:"statusCode"`
	Data       interface{}    `json:"data"`
	Error      *ResponseError `json:"error"`
	Meta       ResponseMeta   `json:"meta"`
}

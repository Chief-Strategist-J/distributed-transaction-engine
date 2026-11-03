package constants

const (
	HeaderTraceparent    = "traceparent"
	HeaderXRequestID     = "x-request-id"
	HeaderXCorrelationID = "x-correlation-id"
	HeaderXCausationID   = "x-causation-id"
	HeaderXIdempotencyKey = "x-idempotency-key"
	HeaderXTenantID      = "x-tenant-id"
	HeaderXClientID      = "x-client-id"
	HeaderXUserID        = "x-user-id"
	HeaderXAPIVersion    = "x-api-version"
	HeaderIfMatch        = "if-match"
	HeaderETag           = "etag"
	HeaderRetryAfter     = "retry-after"
)

const (
	ErrCodeValidationFailed    = "VALIDATION_FAILED"
	ErrCodeConflict            = "CONFLICT"
	ErrCodeNotFound            = "RESOURCE_NOT_FOUND"
	ErrCodePreconditionFailed  = "PRECONDITION_FAILED"
	ErrCodeInternalError       = "INTERNAL_SERVER_ERROR"
)

const (
	StatusAlive         = "alive"
	StatusReady         = "ready"
	StatusDBUnreachable = "database unreachable"
)

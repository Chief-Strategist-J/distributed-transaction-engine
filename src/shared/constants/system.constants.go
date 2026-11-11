package constants

const (
	HeaderTraceparent     = "traceparent"
	HeaderXRequestID      = "x-request-id"
	HeaderXCorrelationID  = "x-correlation-id"
	HeaderXCausationID    = "x-causation-id"
	HeaderXIdempotencyKey = "x-idempotency-key"
	HeaderXTenantID       = "x-tenant-id"
	HeaderXClientID       = "x-client-id"
	HeaderXUserID         = "x-user-id"
	HeaderXAPIVersion     = "x-api-version"
	QueryParamLimit       = "limit"
	HeaderIfMatch         = "if-match"
	HeaderETag            = "etag"
	HeaderRetryAfter      = "retry-after"
)

const (
	ErrCodeValidationFailed   = "VALIDATION_FAILED"
	ErrCodeConflict           = "CONFLICT"
	ErrCodeNotFound           = "RESOURCE_NOT_FOUND"
	ErrCodePreconditionFailed = "PRECONDITION_FAILED"
	ErrCodeInternalError      = "INTERNAL_SERVER_ERROR"
	ErrCodeQuotaExceeded      = "QUOTA_EXCEEDED"

	ErrCodeTenantSuspendedBilling = "TENANT_SUSPENDED_BILLING"
	ErrCodeTenantSuspendedAbuse   = "TENANT_SUSPENDED_ABUSE"
	ErrCodeTenantOffboarding      = "TENANT_OFFBOARDING"
	ErrCodeTenantOffboarded       = "TENANT_OFFBOARDED"
)

const (
	StatusAlive            = "alive"
	StatusReady            = "ready"
	StatusDBUnreachable    = "database unreachable"
	StatusKafkaUnreachable = "kafka unreachable"
)

const (
	TQErrInvalidSyntax           = "INVALID_SYNTAX"
	TQErrUnknownField            = "UNKNOWN_FIELD"
	TQErrOperatorNotAllowed      = "OPERATOR_NOT_ALLOWED"
	TQErrTypeMismatch            = "TYPE_MISMATCH"
	TQErrUnknownEnumValue        = "UNKNOWN_ENUM_VALUE"
	TQErrNestedFieldOutsideQuant = "NESTED_FIELD_OUTSIDE_QUANTIFIER"
	TQErrMoneyCurrencyRequired   = "MONEY_CURRENCY_REQUIRED"
	TQErrTimezoneRequired        = "TIMEZONE_REQUIRED"
	TQErrPrefixTooShort          = "PREFIX_TOO_SHORT"
	TQErrLimitExceeded           = "LIMIT_EXCEEDED"
	TQErrCursorInvalid           = "CURSOR_INVALID"
	TQErrCursorQueryMismatch     = "CURSOR_QUERY_MISMATCH"
	TQErrCursorExpired           = "CURSOR_EXPIRED"
	TQErrFieldNotPermitted       = "FIELD_NOT_PERMITTED"
	TQErrNotFound                = "NOT_FOUND"
	TQErrQueryTooExpensive       = "QUERY_TOO_EXPENSIVE"
	TQErrTimeRangeRequired       = "TIME_RANGE_REQUIRED"
	TQErrResultWindowExceeded    = "RESULT_WINDOW_EXCEEDED"
	TQErrRateLimited             = "RATE_LIMITED"
	TQErrStoreUnavailable        = "STORE_UNAVAILABLE"
	TQErrQueryTimeout            = "QUERY_TIMEOUT"
	TQErrUnsupportedTQLVersion   = "UNSUPPORTED_TQL_VERSION"
	TQErrSavedQueryBroken        = "SAVED_QUERY_BROKEN"
	TQErrExportNotReady          = "EXPORT_NOT_READY"
	TQErrExportLinkInvalid       = "EXPORT_LINK_INVALID"
	TQErrConsistencyTokenInvalid = "CONSISTENCY_TOKEN_INVALID"
)

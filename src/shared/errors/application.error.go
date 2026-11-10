package errors

type ErrorDetail struct {
	Field string
	Issue string
	Rule  string
}

type ApplicationError struct {
	StatusCode        int
	CanonicalCode     string
	Message           string
	Retryable         bool
	Details           []ErrorDetail
	RetryAfterSeconds int
}

func (e *ApplicationError) Error() string {
	return e.Message
}

func NewValidationError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    400,
		CanonicalCode: "VALIDATION_FAILED",
		Message:       message,
		Retryable:     false,
	}
}

func NewBadRequestError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    400,
		CanonicalCode: "BAD_REQUEST",
		Message:       message,
		Retryable:     false,
	}
}

func NewUnauthorizedError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    401,
		CanonicalCode: "UNAUTHORIZED",
		Message:       message,
		Retryable:     false,
	}
}

func NewForbiddenError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    403,
		CanonicalCode: "FORBIDDEN",
		Message:       message,
		Retryable:     false,
	}
}

func NewNotFoundError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    404,
		CanonicalCode: "RESOURCE_NOT_FOUND",
		Message:       message,
		Retryable:     false,
	}
}

func NewConflictError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    409,
		CanonicalCode: "CONFLICT",
		Message:       message,
		Retryable:     false,
	}
}

func NewPreconditionFailedError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    412,
		CanonicalCode: "PRECONDITION_FAILED",
		Message:       message,
		Retryable:     false,
	}
}

func NewInternalError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    500,
		CanonicalCode: "INTERNAL_SERVER_ERROR",
		Message:       message,
		Retryable:     false,
	}
}

func NewQuotaExceededError(message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    429,
		CanonicalCode: "QUOTA_EXCEEDED",
		Message:       message,
		Retryable:     true,
	}
}

func NewTenantRejectedError(canonicalCode string, message string) *ApplicationError {
	return &ApplicationError{
		StatusCode:    403,
		CanonicalCode: canonicalCode,
		Message:       message,
		Retryable:     false,
	}
}

func NewTypedError(statusCode int, canonicalCode string, message string, details ...ErrorDetail) *ApplicationError {
	return &ApplicationError{StatusCode: statusCode, CanonicalCode: canonicalCode, Message: message, Details: details}
}

func NewRateLimitedError(message string, retryAfterSeconds int) *ApplicationError {
	return &ApplicationError{
		StatusCode:        429,
		CanonicalCode:     "RATE_LIMITED",
		Message:           message,
		Retryable:         true,
		RetryAfterSeconds: retryAfterSeconds,
	}
}

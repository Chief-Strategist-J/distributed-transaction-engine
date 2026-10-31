package errors

type ApplicationError struct {
	StatusCode    int
	CanonicalCode string
	Message       string
	Retryable     bool
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

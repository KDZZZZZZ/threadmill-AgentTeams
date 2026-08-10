package kernel

import "errors"

type ErrorCode string

const (
	CodeUnauthorized        ErrorCode = "unauthorized"
	CodeForbidden           ErrorCode = "forbidden"
	CodeInvalidRequest      ErrorCode = "invalid_request"
	CodeCSRFInvalid         ErrorCode = "csrf_invalid"
	CodeOriginInvalid       ErrorCode = "origin_invalid"
	CodeNotFound            ErrorCode = "not_found"
	CodeRevisionConflict    ErrorCode = "revision_conflict"
	CodeIdempotencyConflict ErrorCode = "idempotency_conflict"
	CodeCommandConflict     ErrorCode = "command_conflict"
	CodeStaleBinding        ErrorCode = "stale_binding"
	CodeStaleCommand        ErrorCode = "stale_command"
	CodeStaleCheckpoint     ErrorCode = "stale_checkpoint"
	CodeLeaseConflict       ErrorCode = "lease_conflict"
	CodeExecutorUnavailable ErrorCode = "executor_unavailable"
	CodeInternalError       ErrorCode = "internal_error"
)

// Error is the stable module-boundary error shape used by HTTP, MCP, Runtime,
// and core services. Transport adapters map Code to protocol-specific status.
type Error struct {
	Code        ErrorCode         `json:"code"`
	Message     string            `json:"message"`
	Recoverable bool              `json:"recoverable"`
	Details     map[string]string `json:"details,omitempty"`
}

func (e Error) Error() string {
	if e.Message != "" {
		return string(e.Code) + ": " + e.Message
	}
	return string(e.Code)
}

func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	var coded Error
	if errors.As(err, &coded) {
		return coded.Code
	}
	var codedPtr *Error
	if errors.As(err, &codedPtr) && codedPtr != nil {
		return codedPtr.Code
	}
	return CodeInternalError
}

func IsCode(err error, code ErrorCode) bool {
	return ErrorCodeOf(err) == code
}

func Forbidden(message string) error {
	return Error{Code: CodeForbidden, Message: message, Recoverable: false}
}

func InvalidArgument(message string) error {
	return Error{Code: CodeInvalidRequest, Message: message, Recoverable: false}
}

func IdempotencyConflict() error {
	return Error{
		Code:        CodeIdempotencyConflict,
		Message:     "idempotency key was already used with a different payload",
		Recoverable: false,
	}
}

func StaleBinding(message string) error {
	return Error{Code: CodeStaleBinding, Message: message, Recoverable: true}
}

func LeaseConflict(message string) error {
	return Error{Code: CodeLeaseConflict, Message: message, Recoverable: true}
}

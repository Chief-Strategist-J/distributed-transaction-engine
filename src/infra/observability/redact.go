package observability

import (
	"regexp"
	"strings"
)

const (
	redactedMarker      = "[redacted]"
	maxSanitizedMessage = 300
)

var redactionPatterns = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]*`), redactedMarker},
	{regexp.MustCompile(`(?i)\b(password|passwd|secret|token|api[_-]?key|authorization)\b\s*[=:]\s*(?:bearer\s+)?\S+`), "${1}=" + redactedMarker},
	{regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._\-]+`), "bearer " + redactedMarker},
	{regexp.MustCompile(`\bapk_[A-Za-z0-9_]+`), redactedMarker},
	{regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`), redactedMarker},
	{regexp.MustCompile(`\([^()]*\)=\([^()]*\)`), "(" + redactedMarker + ")"},
	{regexp.MustCompile(`\b\d{9,}\b`), redactedMarker},
	{regexp.MustCompile(`\b(?:\d[ -]?){12,18}\d\b`), redactedMarker},
}

func SanitizeText(raw string) string {
	cleaned := strings.Map(func(character rune) rune {
		if character == '\n' || character == '\r' || character == '\t' {
			return ' '
		}
		if character < 0x20 {
			return -1
		}
		return character
	}, raw)
	for _, rule := range redactionPatterns {
		cleaned = rule.pattern.ReplaceAllString(cleaned, rule.replacement)
	}
	runes := []rune(strings.TrimSpace(cleaned))
	if len(runes) > maxSanitizedMessage {
		return string(runes[:maxSanitizedMessage]) + "..."
	}
	return string(runes)
}

type sanitizedError struct {
	original error
	message  string
}

func newSanitizedError(err error) error {
	return &sanitizedError{original: err, message: SanitizeText(err.Error())}
}

func (e *sanitizedError) Error() string {
	return e.message
}

func (e *sanitizedError) Unwrap() error {
	return e.original
}

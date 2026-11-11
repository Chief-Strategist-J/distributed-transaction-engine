package chassis

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const (
	maxHeldErrorBody   = 64 << 10
	genericServerError = "internal server error"
	genericClientError = "the request could not be processed"
)

var leakPattern = regexp.MustCompile(`(?i)(\bpq:|sqlstate|violates (unique|foreign key|check|not-null)|duplicate key value|relation "|column "|constraint "|syntax error at or near|\bselect\b.+\bfrom\b|\binsert\s+into\b|\bupdate\b.+\bset\b|pg_catalog|information_schema)`)

func RequestSizeLimit(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxBytes {
			writeProblem(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds the allowed size")
			return
		}
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)
			return
		}
		payload, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
		_ = r.Body.Close()
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "VALIDATION_FAILED", "request body could not be read")
			return
		}
		if int64(len(payload)) > maxBytes {
			writeProblem(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds the allowed size")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(payload))
		next.ServeHTTP(w, r)
	})
}

func writeProblem(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": false, "statusCode": status, "data": nil,
		"error": map[string]any{"code": code, "message": message, "retryable": false},
	})
}

type sanitizingWriter struct {
	http.ResponseWriter
	status int
	held   bool
	body   bytes.Buffer
}

func (w *sanitizingWriter) WriteHeader(code int) {
	w.status = code
	if code >= http.StatusBadRequest {
		w.held = true
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *sanitizingWriter) Write(payload []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if !w.held {
		return w.ResponseWriter.Write(payload)
	}
	if w.body.Len()+len(payload) <= maxHeldErrorBody {
		w.body.Write(payload)
	}
	return len(payload), nil
}

func (w *sanitizingWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok && !w.held {
		flusher.Flush()
	}
}

func (w *sanitizingWriter) finish(hideServerDetails bool) {
	if !w.held {
		return
	}
	body := rewriteErrorBody(w.body.Bytes(), w.status, hideServerDetails)
	w.ResponseWriter.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(w.status)
	_, _ = w.ResponseWriter.Write(body)
}

func rewriteErrorBody(body []byte, status int, hideServerDetails bool) []byte {
	var envelope map[string]any
	if json.Unmarshal(body, &envelope) != nil {
		if len(body) == 0 || leakPattern.Match(body) {
			return genericEnvelope(status)
		}
		return body
	}
	errorObject, _ := envelope["error"].(map[string]any)
	if errorObject == nil {
		return body
	}
	message, _ := errorObject["message"].(string)
	if leakPattern.MatchString(message) || (hideServerDetails && status >= http.StatusInternalServerError) {
		errorObject["message"] = genericMessage(status)
		delete(errorObject, "details")
		rewritten, err := json.Marshal(envelope)
		if err != nil {
			return genericEnvelope(status)
		}
		return rewritten
	}
	return body
}

func genericMessage(status int) string {
	if status >= http.StatusInternalServerError {
		return genericServerError
	}
	return genericClientError
}

func genericEnvelope(status int) []byte {
	encoded, _ := json.Marshal(map[string]any{
		"success": false, "statusCode": status, "data": nil,
		"error": map[string]any{"code": strings.ToUpper(http.StatusText(status)), "message": genericMessage(status), "retryable": false},
	})
	return encoded
}

func ErrorSanitizer(hideServerDetails bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := &sanitizingWriter{ResponseWriter: w}
		defer writer.finish(hideServerDetails)
		next.ServeHTTP(writer, r)
	})
}

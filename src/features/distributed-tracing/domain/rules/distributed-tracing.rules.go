package rules

import (
	"crypto/rand"
	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

func ParseTraceparent(header string) (*contracts.TraceContext, bool) {
	defer observability.TraceEnter(nil)()
	trimmed := strings.TrimSpace(header)
	parts := strings.Split(trimmed, "-")
	if len(parts) != 4 {
		return nil, false
	}

	version := parts[0]
	traceID := parts[1]
	parentSpanID := parts[2]
	flagsHex := parts[3]

	if version != "00" {
		return nil, false
	}
	if len(traceID) != 32 || traceID == "00000000000000000000000000000000" {
		return nil, false
	}
	if _, err := hex.DecodeString(traceID); err != nil {
		return nil, false
	}
	if len(parentSpanID) != 16 || parentSpanID == "0000000000000000" {
		return nil, false
	}
	if _, err := hex.DecodeString(parentSpanID); err != nil {
		return nil, false
	}
	flagBytes, err := hex.DecodeString(flagsHex)
	if err != nil || len(flagBytes) != 1 {
		return nil, false
	}

	flags := flagBytes[0]
	isSampled := (flags & 0x01) == 0x01

	return &contracts.TraceContext{
		TraceID:    traceID,
		SpanID:     parentSpanID,
		TraceFlags: flags,
		IsSampled:  isSampled,
		Baggage:    make(map[string]string),
	}, true
}

func FormatTraceparent(tc *contracts.TraceContext) string {
	defer observability.TraceEnter(nil)()
	if tc == nil {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-%02x", tc.TraceID, tc.SpanID, tc.TraceFlags)
}

func GenerateTraceID() string {
	defer observability.TraceEnter(nil)()
	b := make([]byte, 16)
	_, swallowedErr1 := rand.Read(b)
	observability.NoteSwallowed(swallowedErr1)
	return hex.EncodeToString(b)
}

func GenerateSpanID() string {
	defer observability.TraceEnter(nil)()
	b := make([]byte, 8)
	_, swallowedErr2 := rand.Read(b)
	observability.NoteSwallowed(swallowedErr2)
	return hex.EncodeToString(b)
}

func EvaluateAdaptiveSampler(query contracts.EvaluateSamplingQuery, baseRate float64) bool {
	defer observability.TraceEnter(nil)()
	if query.IsError || query.IsCompensation || query.IsVIP {
		return true
	}
	if baseRate >= 1.0 {
		return true
	}
	if baseRate <= 0.0 {
		return false
	}

	corrBytes := query.CorrelationID[:]
	if len(corrBytes) >= 8 {
		hashVal := binary.BigEndian.Uint64(corrBytes[:8])
		threshold := uint64(baseRate * float64(18446744073709551615))
		return hashVal <= threshold
	}
	return false
}

func ParseBaggage(header string) map[string]string {
	defer observability.TraceEnter(nil)()
	baggage := make(map[string]string)
	if strings.TrimSpace(header) == "" {
		return baggage
	}

	allowList := map[string]struct{}{
		"tenant_tier": {},
		"session_id":  {},
		"deep_mode":   {},
		"client_type": {},
	}

	items := strings.Split(header, ",")
	for _, item := range items {
		kv := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(kv) == 2 {
			k := strings.ToLower(strings.TrimSpace(kv[0]))
			v := strings.TrimSpace(kv[1])
			if _, allowed := allowList[k]; allowed {
				baggage[k] = v
			}
		}
	}
	return baggage
}

func SanitizeAttributes(attrs map[string]interface{}) map[string]interface{} {
	defer observability.TraceEnter(nil)()
	if attrs == nil {
		return map[string]interface{}{}
	}
	sensitiveKeys := map[string]struct{}{
		"password":      {},
		"secret":        {},
		"token":         {},
		"ssn":           {},
		"authorization": {},
		"api_key":       {},
	}
	sanitized := make(map[string]interface{}, len(attrs))
	for k, v := range attrs {
		lowerKey := strings.ToLower(k)
		if _, exists := sensitiveKeys[lowerKey]; exists {
			sanitized[k] = "******"
			continue
		}
		sanitized[k] = v
	}
	return sanitized
}

func BuildSpanEvent(name string, attrs map[string]interface{}) contracts.SpanEvent {
	defer observability.TraceEnter(nil)()
	return contracts.SpanEvent{
		Name:       name,
		Timestamp:  time.Now().UTC(),
		Attributes: SanitizeAttributes(attrs),
	}
}

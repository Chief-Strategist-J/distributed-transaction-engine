package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/features/distributed-tracing/domain/rules"
	"github.com/google/uuid"
)

func TestParseTraceparent_Valid(t *testing.T) {
	raw := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tc, ok := rules.ParseTraceparent(raw)
	if !ok {
		t.Fatal("expected valid traceparent to parse successfully")
	}
	if tc.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("expected trace ID match, got %s", tc.TraceID)
	}
	if tc.SpanID != "00f067aa0ba902b7" {
		t.Fatalf("expected span ID match, got %s", tc.SpanID)
	}
	if !tc.IsSampled {
		t.Fatal("expected sampled flag true")
	}

	formatted := rules.FormatTraceparent(tc)
	if formatted != raw {
		t.Fatalf("expected roundtrip format %s, got %s", raw, formatted)
	}
}

func TestParseTraceparent_InvalidFormats(t *testing.T) {
	invalids := []string{
		"",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"00-short-00f067aa0ba902b7-01",
		"not-a-traceparent",
	}

	for _, inv := range invalids {
		_, ok := rules.ParseTraceparent(inv)
		if ok {
			t.Fatalf("expected parsing to fail for %q", inv)
		}
	}
}

func TestEvaluateAdaptiveSampler_GuaranteedCaptures(t *testing.T) {
	corrID := uuid.New()

	sampled := rules.EvaluateAdaptiveSampler(contracts.EvaluateSamplingQuery{
		TenantID:      "t1",
		CorrelationID: corrID,
		IsError:       true,
	}, 0.0)
	if !sampled {
		t.Fatal("expected 100% sampling on error")
	}

	sampled = rules.EvaluateAdaptiveSampler(contracts.EvaluateSamplingQuery{
		TenantID:       "t1",
		CorrelationID:  corrID,
		IsCompensation: true,
	}, 0.0)
	if !sampled {
		t.Fatal("expected 100% sampling on compensation")
	}

	sampled = rules.EvaluateAdaptiveSampler(contracts.EvaluateSamplingQuery{
		TenantID:      "t1",
		CorrelationID: corrID,
		IsVIP:         true,
	}, 0.0)
	if !sampled {
		t.Fatal("expected 100% sampling on VIP tenant")
	}
}

func TestParseBaggage_AllowListing(t *testing.T) {
	header := "tenant_tier=platinum, session_id=sess_123, user_email=leak@domain.com, deep_mode=true"
	baggage := rules.ParseBaggage(header)

	if baggage["tenant_tier"] != "platinum" {
		t.Fatalf("expected tenant_tier, got %v", baggage["tenant_tier"])
	}
	if baggage["deep_mode"] != "true" {
		t.Fatalf("expected deep_mode, got %v", baggage["deep_mode"])
	}
	if _, exists := baggage["user_email"]; exists {
		t.Fatal("expected un-allowlisted PII user_email to be stripped")
	}
}

func TestSanitizeAttributes(t *testing.T) {
	attrs := map[string]interface{}{
		"http.method":   "POST",
		"http.route":    "/api/v1/orders",
		"password":      "secret123",
		"authorization": "Bearer token_abc",
	}

	sanitized := rules.SanitizeAttributes(attrs)
	if sanitized["password"] != "******" {
		t.Fatalf("expected masked password, got %v", sanitized["password"])
	}
	if sanitized["authorization"] != "******" {
		t.Fatalf("expected masked authorization, got %v", sanitized["authorization"])
	}
	if sanitized["http.method"] != "POST" {
		t.Fatalf("expected preserved http.method, got %v", sanitized["http.method"])
	}
}


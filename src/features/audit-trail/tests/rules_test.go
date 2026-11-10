package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/audit-trail/domain/rules"
	"github.com/google/uuid"
)

func TestCanonicalEncodeRecord_DifferentiatesBoundaries(t *testing.T) {
	segID := uuid.New()
	now := time.Now().UTC()
	key := []byte("test-secret-key-32-bytes-long!")

	hash1, err := rules.ComputeKeyedEventHash("prev1", segID, 1, "k1", now, "t1", "ab", "c", map[string]interface{}{"foo": "bar"}, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hash2, err := rules.ComputeKeyedEventHash("prev1", segID, 1, "k1", now, "t1", "a", "bc", map[string]interface{}{"foo": "bar"}, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hash1 == hash2 {
		t.Fatalf("expected different hashes for boundary differences, got identical: %s", hash1)
	}
}

func TestValidateAttribution_RejectsDefaultsAndEmpty(t *testing.T) {
	valid, _ := rules.ValidateAttribution("", "tenant-1")
	if valid {
		t.Fatal("expected empty actor to fail validation")
	}

	valid, _ = rules.ValidateAttribution("actor-1", "")
	if valid {
		t.Fatal("expected empty tenant to fail validation")
	}

	valid, _ = rules.ValidateAttribution("SYSTEM_KERNEL", "tenant-1")
	if valid {
		t.Fatal("expected SYSTEM_KERNEL default to fail validation")
	}

	valid, _ = rules.ValidateAttribution("GLOBAL_SYSTEM", "tenant-1")
	if valid {
		t.Fatal("expected GLOBAL_SYSTEM default to fail validation")
	}

	valid, _ = rules.ValidateAttribution("job-nightly-reconcile-001", "tenant-1")
	if !valid {
		t.Fatal("expected explicit job actor to pass validation")
	}
}

func TestMaskSensitivePayload(t *testing.T) {
	input := map[string]interface{}{
		"amount":   100.50,
		"password": "super-secret-password",
		"token":    "jwt.token.here",
		"metadata": map[string]interface{}{
			"api_key": "raw-key-123",
			"note":    "public info",
		},
	}

	masked := rules.MaskSensitivePayload(input)
	if masked["password"] != "******" {
		t.Fatalf("expected masked password, got %v", masked["password"])
	}
	if masked["token"] != "******" {
		t.Fatalf("expected masked token, got %v", masked["token"])
	}

	nested, ok := masked["metadata"].(map[string]interface{})
	if !ok {
		t.Fatal("expected nested metadata map")
	}
	if nested["api_key"] != "******" {
		t.Fatalf("expected masked nested api_key, got %v", nested["api_key"])
	}
	if nested["note"] != "public info" {
		t.Fatalf("expected unmasked note, got %v", nested["note"])
	}
}


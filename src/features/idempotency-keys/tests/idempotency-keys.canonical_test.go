package tests

import (
	"encoding/json"
	"testing"

	"distributed-transaction-engine/src/features/idempotency-keys/contracts"
)

func fingerprintOf(t *testing.T, raw string) string {
	t.Helper()
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := contracts.ComputeCanonicalFingerprint("op", payload)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func TestFingerprintIgnoresObjectKeyOrderAtEveryDepth(t *testing.T) {
	variants := []string{
		`{"a":1,"b":{"x":1,"y":[{"p":1,"q":2},{"q":2,"p":1}]},"c":"z"}`,
		`{"c":"z","b":{"y":[{"q":2,"p":1},{"p":1,"q":2}],"x":1},"a":1}`,
		`{"b":{"x":1,"y":[{"p":1,"q":2},{"p":1,"q":2}]},"a":1,"c":"z"}`,
		"{ \"a\" : 1 ,\n \"b\" : { \"y\" : [ {\"p\":1,\"q\":2}, {\"p\":1,\"q\":2} ], \"x\" : 1 }, \"c\" : \"z\" }",
	}
	expected := fingerprintOf(t, variants[0])
	for _, variant := range variants[1:] {
		if got := fingerprintOf(t, variant); got != expected {
			t.Fatalf("key order or whitespace changed the fingerprint for %s", variant)
		}
	}
}

func TestFingerprintStillDistinguishesDifferentContent(t *testing.T) {
	base := fingerprintOf(t, `{"a":1,"b":[1,2]}`)
	for _, different := range []string{`{"a":2,"b":[1,2]}`, `{"a":1,"b":[2,1]}`, `{"a":1,"b":[1,2],"c":null}`, `{"a":"1","b":[1,2]}`} {
		if fingerprintOf(t, different) == base {
			t.Fatalf("semantically different payload collided: %s", different)
		}
	}
}

func TestFingerprintSortsKeysByUTF16CodeUnitsLikeRFC8785(t *testing.T) {
	first := fingerprintOf(t, "{\"\\ue000\":1,\"\\ud83d\\ude00\":2,\"a\":3}")
	second := fingerprintOf(t, "{\"a\":3,\"\\ud83d\\ude00\":2,\"\\ue000\":1}")
	if first != second {
		t.Fatal("RFC 8785 ordering must be deterministic regardless of input order")
	}
	if contracts.CanonicalKeyOrderForTest([]string{"\ue000", "\U0001F600", "a"})[1] != "\U0001F600" {
		t.Fatal("a supplementary-plane key (surrogate pair 0xD83D) must sort before U+E000 in UTF-16 order")
	}
}

func TestFingerprintIsScopedToTheOperation(t *testing.T) {
	var payload map[string]interface{}
	_ = json.Unmarshal([]byte(`{"a":1}`), &payload)
	one, _ := contracts.ComputeCanonicalFingerprint("orders.checkout", payload)
	two, _ := contracts.ComputeCanonicalFingerprint("orders.refund", payload)
	if one == two {
		t.Fatal("the same payload on different operations must not share a fingerprint")
	}
}

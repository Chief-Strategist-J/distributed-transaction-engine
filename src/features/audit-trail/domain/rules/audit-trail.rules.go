package rules

import (
	"crypto/hmac"
	"crypto/sha256"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"sort"
	"strings"
	"time"
)

func ComputeGenesisHash(tenantID string, segmentID uuid.UUID, key []byte) string {
	defer observability.TraceEnter(nil)()
	mac := hmac.New(sha256.New, key)
	seed := fmt.Sprintf("%d:GENESIS:%d:%s:%d:%s", len("GENESIS"), len(tenantID), tenantID, len(segmentID.String()), segmentID.String())
	mac.Write([]byte(seed))
	return hex.EncodeToString(mac.Sum(nil))
}

func CanonicalEncodeRecord(prevHash string, segmentID uuid.UUID, seqID int64, keyID string, occurredAt time.Time, tenantID string, actorID string, action string, payload map[string]interface{}) (spanResult0 []byte, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	canonicalJSON, err := CanonicalJSONMarshal(payload)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to serialize canonical payload: " + err.Error())
	}
	tsStr := occurredAt.UTC().Format(time.RFC3339Nano)
	seqStr := fmt.Sprintf("%d", seqID)
	segStr := segmentID.String()

	var buf strings.Builder
	writeField(&buf, prevHash)
	writeField(&buf, segStr)
	writeField(&buf, seqStr)
	writeField(&buf, keyID)
	writeField(&buf, tsStr)
	writeField(&buf, tenantID)
	writeField(&buf, actorID)
	writeField(&buf, action)
	writeField(&buf, string(canonicalJSON))

	return []byte(buf.String()), nil
}

func writeField(buf *strings.Builder, val string) {
	buf.WriteString(fmt.Sprintf("%d:%s;", len(val), val))
}

func ComputeKeyedEventHash(prevHash string, segmentID uuid.UUID, seqID int64, keyID string, occurredAt time.Time, tenantID string, actorID string, action string, payload map[string]interface{}, secretKey []byte) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	data, err := CanonicalEncodeRecord(prevHash, segmentID, seqID, keyID, occurredAt, tenantID, actorID, action, payload)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secretKey)
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func VerifyEventNode(prevHash string, storedCurrHash string, segmentID uuid.UUID, seqID int64, keyID string, occurredAt time.Time, tenantID string, actorID string, action string, payload map[string]interface{}, secretKey []byte) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	recomputed, err := ComputeKeyedEventHash(prevHash, segmentID, seqID, keyID, occurredAt, tenantID, actorID, action, payload, secretKey)
	if err != nil {
		return false, err
	}
	return hmac.Equal([]byte(recomputed), []byte(storedCurrHash)), nil
}

func CanonicalJSONMarshal(v map[string]interface{}) (spanResult0 []byte, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if v == nil {
		return []byte("{}"), nil
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]interface{}, len(v))
	for _, k := range keys {
		ordered[k] = v[k]
	}
	return json.Marshal(ordered)
}

func ValidateAttribution(actorID string, tenantID string) (bool, string) {
	defer observability.TraceEnter(nil)()
	cleanActor := strings.TrimSpace(actorID)
	cleanTenant := strings.TrimSpace(tenantID)
	if cleanActor == "" {
		return false, "actor_id cannot be empty"
	}
	if cleanTenant == "" {
		return false, "tenant_id cannot be empty"
	}
	if strings.EqualFold(cleanActor, "SYSTEM_KERNEL") || strings.EqualFold(cleanActor, "GLOBAL_SYSTEM") {
		return false, "synthetic system defaults are forbidden without job declaration"
	}
	return true, ""
}

func MaskSensitivePayload(payload map[string]interface{}) map[string]interface{} {
	defer observability.TraceEnter(nil)()
	if payload == nil {
		return map[string]interface{}{}
	}
	sensitiveKeys := map[string]struct{}{
		"password":     {},
		"secret":       {},
		"token":        {},
		"ssn":          {},
		"credit_card":  {},
		"card_number":  {},
		"api_key":      {},
		"access_token": {},
	}
	masked := make(map[string]interface{}, len(payload))
	for k, v := range payload {
		lowerKey := strings.ToLower(k)
		if _, exists := sensitiveKeys[lowerKey]; exists {
			masked[k] = "******"
			continue
		}
		if nestedMap, ok := v.(map[string]interface{}); ok {
			masked[k] = MaskSensitivePayload(nestedMap)
			continue
		}
		masked[k] = v
	}
	return masked
}

package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	appErrors "distributed-transaction-engine/src/shared/errors"

	"github.com/google/uuid"
)

var (
	idkPublicIDPattern = regexp.MustCompile(`^idk_[0-9a-zA-Z]{26}$`)
)

func ValidateIngestInput(input IngestIdempotencyInput) (*uuid.UUID, *uuid.UUID, error) {
	if strings.TrimSpace(input.ID) == "" {
		return nil, nil, appErrors.NewValidationError("id cannot be empty")
	}
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return nil, nil, appErrors.NewValidationError("id must be a valid uuid")
	}

	if !idkPublicIDPattern.MatchString(input.PublicID) {
		return nil, nil, appErrors.NewValidationError("invalid publicId format: must match idk_ + 26 alphanumeric chars")
	}

	if strings.TrimSpace(input.TenantID) == "" {
		return nil, nil, appErrors.NewValidationError("tenantId cannot be empty")
	}

	if strings.TrimSpace(input.Source) == "" {
		return nil, nil, appErrors.NewValidationError("source cannot be empty")
	}

	if strings.TrimSpace(input.ExternalID) == "" {
		return nil, nil, appErrors.NewValidationError("externalId cannot be empty")
	}

	if strings.TrimSpace(input.CorrelationID) == "" {
		return nil, nil, appErrors.NewValidationError("correlationId cannot be empty")
	}
	corrID, err := uuid.Parse(input.CorrelationID)
	if err != nil {
		return nil, nil, appErrors.NewValidationError("correlationId must be a valid uuid")
	}

	if strings.TrimSpace(input.PrincipalID) == "" {
		return nil, nil, appErrors.NewValidationError("principalId cannot be empty")
	}

	if strings.TrimSpace(input.OperationID) == "" {
		return nil, nil, appErrors.NewValidationError("operationId cannot be empty")
	}

	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return nil, nil, appErrors.NewValidationError("idempotencyKey cannot be empty")
	}

	if len(input.IdempotencyKey) > 128 {
		return nil, nil, appErrors.NewValidationError("idempotencyKey exceeds maximum length of 128 characters")
	}

	if input.Payload == nil {
		return nil, nil, appErrors.NewValidationError("payload cannot be null")
	}

	return &id, &corrID, nil
}

func ComputeCanonicalFingerprint(operationID string, payload map[string]interface{}) (string, error) {
	canonicalJSON, err := canonicalizeValue(payload)
	if err != nil {
		return "", err
	}

	hashInput := fmt.Sprintf("%s|%s", strings.TrimSpace(operationID), string(canonicalJSON))
	sum := sha256.Sum256([]byte(hashInput))
	return hex.EncodeToString(sum[:]), nil
}

func canonicalizeValue(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case map[string]interface{}:
		return canonicalizeMap(val)
	case []interface{}:
		return canonicalizeSlice(val)
	default:
		return json.Marshal(val)
	}
}

func canonicalizeMap(m map[string]interface{}) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf strings.Builder
	buf.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			buf.WriteString(",")
		}
		keyBytes, _ := json.Marshal(k)
		buf.Write(keyBytes)
		buf.WriteString(":")
		valBytes, err := canonicalizeValue(m[k])
		if err != nil {
			return nil, err
		}
		buf.Write(valBytes)
	}
	buf.WriteString("}")
	return []byte(buf.String()), nil
}

func canonicalizeSlice(s []interface{}) ([]byte, error) {
	var buf strings.Builder
	buf.WriteString("[")
	for i, item := range s {
		if i > 0 {
			buf.WriteString(",")
		}
		itemBytes, err := canonicalizeValue(item)
		if err != nil {
			return nil, err
		}
		buf.Write(itemBytes)
	}
	buf.WriteString("]")
	return []byte(buf.String()), nil
}


package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"

	"distributed-transaction-engine/src/shared/errors"
)

var publicIDPattern = regexp.MustCompile(`^inb_[0-9a-zA-Z]{26}$`)

func ComputeCanonicalPayloadHash(payload map[string]interface{}) (string, error) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", errors.NewValidationError("failed to serialize canonical payload")
	}
	hash := sha256.Sum256(bytes)
	return hex.EncodeToString(hash[:]), nil
}

func ValidateIngestMessageInput(input *IngestMessageInput) error {
	if input == nil {
		return errors.NewValidationError("ingest message input cannot be nil")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId cannot be empty")
	}
	if input.Source == "" {
		return errors.NewValidationError("source cannot be empty")
	}
	if input.ExternalID == "" {
		return errors.NewValidationError("externalId cannot be empty")
	}
	if input.HandlerName == "" {
		return errors.NewValidationError("handlerName cannot be empty")
	}
	if input.HandlerGeneration < 1 {
		return errors.NewValidationError("handlerGeneration must be at least 1")
	}
	if !publicIDPattern.MatchString(input.PublicID) {
		return errors.NewValidationError("invalid publicId format: must match inb_ + 26 alphanumeric chars")
	}
	if input.ProducerEventTime.IsZero() {
		return errors.NewValidationError("producerEventTime must be specified")
	}
	return nil
}


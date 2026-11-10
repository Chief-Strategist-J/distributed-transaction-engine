package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"regexp"
)

var (
	uuidRegex   = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-7][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	oxePubRegex = regexp.MustCompile(`^oxe_[0-9a-zA-Z]{26}$`)
)

const MaxPayloadSizeBytes = 1024 * 1024

func ValidateCreateOutboxEventInput(input *CreateOutboxEventInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if input == nil {
		return errors.NewValidationError("input must not be nil")
	}
	if !uuidRegex.MatchString(input.ID) {
		return errors.NewValidationError("id must be a valid UUIDv7")
	}
	if !oxePubRegex.MatchString(input.PublicID) {
		return errors.NewValidationError("publicId must match pattern ^oxe_[0-9a-zA-Z]{26}$")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId must not be empty")
	}
	if !uuidRegex.MatchString(input.SagaID) {
		return errors.NewValidationError("sagaId must be a valid UUID")
	}
	if input.AggregateType == "" {
		return errors.NewValidationError("aggregateType must not be empty")
	}
	if input.AggregateID == "" {
		return errors.NewValidationError("aggregateId must not be empty")
	}
	if input.AggregateSequence <= 0 {
		return errors.NewValidationError("aggregateSequence must be strictly positive (gapless sequence >= 1)")
	}
	if input.DestinationTopic == "" {
		return errors.NewValidationError("destinationTopic must not be empty")
	}
	if input.PartitionKey == "" {
		return errors.NewValidationError("partitionKey must not be empty")
	}
	if input.EventType == "" {
		return errors.NewValidationError("eventType must not be empty")
	}
	if input.Payload == nil {
		return errors.NewValidationError("payload must not be nil")
	}

	payloadBytes, err := json.Marshal(input.Payload)
	if err != nil {
		return errors.NewValidationError("failed to serialize payload JSON")
	}
	if len(payloadBytes) > MaxPayloadSizeBytes {
		return errors.NewValidationError("payload exceeds maximum allowable size of 1MB")
	}

	return nil
}

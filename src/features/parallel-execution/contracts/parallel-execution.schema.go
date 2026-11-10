package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"regexp"
)

var (
	uuidRegex   = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-7][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	parPubRegex = regexp.MustCompile(`^par_[0-9a-zA-Z]{26}$`)
)

func ValidateStartParallelGroupInput(input *StartParallelGroupInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if input == nil {
		return errors.NewValidationError("input must not be nil")
	}
	if !uuidRegex.MatchString(input.ID) {
		return errors.NewValidationError("id must be a valid UUIDv7")
	}
	if !parPubRegex.MatchString(input.PublicID) {
		return errors.NewValidationError("publicId must match pattern ^par_[0-9a-zA-Z]{26}$")
	}
	if input.TenantID == "" {
		return errors.NewValidationError("tenantId must not be empty")
	}
	if !uuidRegex.MatchString(input.SagaID) {
		return errors.NewValidationError("sagaId must be a valid UUID")
	}
	if len(input.Branches) == 0 {
		return errors.NewValidationError("group must contain at least one branch")
	}
	if input.JoinPolicy == PolicyNOfM && (input.QuorumCount < 1 || input.QuorumCount > len(input.Branches)) {
		return errors.NewValidationError("quorumCount must be between 1 and total branches for N_OF_M")
	}

	seenBranches := make(map[string]bool)
	for _, b := range input.Branches {
		if b.BranchID == "" {
			return errors.NewValidationError("branchId must not be empty")
		}
		if seenBranches[b.BranchID] {
			return errors.NewValidationError("duplicate branchId detected: " + b.BranchID)
		}
		seenBranches[b.BranchID] = true
	}
	return nil
}

package contracts

import (
	"regexp"
	"strings"

	appErrors "distributed-transaction-engine/src/shared/errors"
)

var (
	leasePublicIDRegex = regexp.MustCompile(`^fnl_[0-9a-zA-Z]{26}$`)
	auditPublicIDRegex = regexp.MustCompile(`^fnv_[0-9a-zA-Z]{26}$`)
)

func ValidateAcquireInput(input AcquireLeaseInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	if strings.TrimSpace(input.ResourceID) == "" {
		return appErrors.NewValidationError("resourceId cannot be empty")
	}
	if strings.TrimSpace(input.HolderID) == "" {
		return appErrors.NewValidationError("holderId cannot be empty")
	}
	if input.LeaseDurationMs < 1000 {
		return appErrors.NewValidationError("leaseDurationMs must be at least 1000")
	}
	return nil
}

func ValidateRenewInput(input RenewLeaseInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	if strings.TrimSpace(input.ResourceID) == "" {
		return appErrors.NewValidationError("resourceId cannot be empty")
	}
	if strings.TrimSpace(input.HolderID) == "" {
		return appErrors.NewValidationError("holderId cannot be empty")
	}
	if input.Epoch < 1 {
		return appErrors.NewValidationError("epoch must be at least 1")
	}
	if input.LeaseDurationMs < 1000 {
		return appErrors.NewValidationError("leaseDurationMs must be at least 1000")
	}
	return nil
}

func ValidateReleaseInput(input ReleaseLeaseInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	if strings.TrimSpace(input.ResourceID) == "" {
		return appErrors.NewValidationError("resourceId cannot be empty")
	}
	if strings.TrimSpace(input.HolderID) == "" {
		return appErrors.NewValidationError("holderId cannot be empty")
	}
	if input.Epoch < 1 {
		return appErrors.NewValidationError("epoch must be at least 1")
	}
	return nil
}

func ValidateBarrierInput(input FencedBarrierInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	if strings.TrimSpace(input.ResourceID) == "" {
		return appErrors.NewValidationError("resourceId cannot be empty")
	}
	if input.Token < 1 {
		return appErrors.NewValidationError("token must be at least 1")
	}
	return nil
}

func ValidateLeasePublicID(publicID string) bool {
	return leasePublicIDRegex.MatchString(publicID)
}

func ValidateAuditPublicID(publicID string) bool {
	return auditPublicIDRegex.MatchString(publicID)
}


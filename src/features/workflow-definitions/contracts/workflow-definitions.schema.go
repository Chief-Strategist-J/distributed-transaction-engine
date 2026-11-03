package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"

	"distributed-transaction-engine/src/shared/errors"
)

var (
	nameRegex     = regexp.MustCompile(`^[a-zA-Z0-9_\-\.]{3,128}$`)
	stepIDRegex   = regexp.MustCompile(`^[a-zA-Z0-9_\-]{2,64}$`)
	publicIDRegex = regexp.MustCompile(`^wfd_[0-9a-zA-Z]{26}$`)
)

func ValidateCreateWorkflowDefinitionInput(input CreateWorkflowDefinitionInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return errors.NewValidationError("tenantId cannot be empty")
	}
	if !nameRegex.MatchString(input.Name) {
		return errors.NewValidationError(fmt.Sprintf("invalid workflow name: %s", input.Name))
	}
	if input.Version < 1 {
		return errors.NewValidationError("version must be greater than or equal to 1")
	}
	if strings.TrimSpace(input.ExternalID) == "" {
		return errors.NewValidationError("externalId cannot be empty")
	}
	if strings.TrimSpace(input.CorrelationID) == "" {
		return errors.NewValidationError("correlationId cannot be empty")
	}
	if len(input.Steps) == 0 {
		return errors.NewValidationError("workflow must define at least one step")
	}

	seenStepIDs := make(map[string]struct{}, len(input.Steps))
	pivotCount := 0

	for _, step := range input.Steps {
		if !stepIDRegex.MatchString(step.StepID) {
			return errors.NewValidationError(fmt.Sprintf("invalid stepId format: %s", step.StepID))
		}
		if _, exists := seenStepIDs[step.StepID]; exists {
			return errors.NewValidationError(fmt.Sprintf("duplicate stepId in workflow: %s", step.StepID))
		}
		seenStepIDs[step.StepID] = struct{}{}

		if strings.TrimSpace(step.StepName) == "" {
			return errors.NewValidationError(fmt.Sprintf("stepName cannot be empty for step: %s", step.StepID))
		}
		if strings.TrimSpace(step.ActionType) == "" || strings.TrimSpace(step.ActionEndpoint) == "" {
			return errors.NewValidationError(fmt.Sprintf("actionType and actionEndpoint required for step: %s", step.StepID))
		}
		if err := validateEndpointSecurity(step.ActionEndpoint); err != nil {
			return err
		}
		if step.TimeoutSeconds < 1 || step.TimeoutSeconds > 86400 {
			return errors.NewValidationError(fmt.Sprintf("timeoutSeconds must be between 1 and 86400 for step: %s", step.StepID))
		}
		if step.MaxRetries < 0 || step.MaxRetries > 100 {
			return errors.NewValidationError(fmt.Sprintf("maxRetries must be between 0 and 100 for step: %s", step.StepID))
		}

		if step.IsPivot {
			pivotCount++
			if strings.TrimSpace(step.CompensationAction) != "" || strings.TrimSpace(step.CompensationEndpoint) != "" {
				return errors.NewValidationError(fmt.Sprintf("pivot step %s must not have compensation action or endpoint", step.StepID))
			}
		} else {
			if strings.TrimSpace(step.CompensationAction) == "" || strings.TrimSpace(step.CompensationEndpoint) == "" {
				return errors.NewValidationError(fmt.Sprintf("non-pivot step %s must declare compensation action and endpoint", step.StepID))
			}
			if err := validateEndpointSecurity(step.CompensationEndpoint); err != nil {
				return err
			}
		}
	}

	if pivotCount > 1 {
		return errors.NewValidationError("workflow cannot declare more than one pivot step")
	}

	return nil
}

func validateEndpointSecurity(endpoint string) error {
	trimmed := strings.TrimSpace(endpoint)
	if trimmed == "" {
		return nil
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return errors.NewValidationError(fmt.Sprintf("malformed endpoint URL: %s", trimmed))
		}
		host := strings.ToLower(u.Hostname())
		if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "169.254.169.254" {
			return errors.NewValidationError(fmt.Sprintf("prohibited loopback or link-local address in endpoint: %s", trimmed))
		}
		ip := net.ParseIP(host)
		if ip != nil {
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				return errors.NewValidationError(fmt.Sprintf("prohibited loopback or link-local IP in endpoint: %s", trimmed))
			}
		}
	}
	return nil
}

func ComputeSpecHash(name string, steps []StepDefinition) (string, error) {
	canonicalData, err := json.Marshal(struct {
		Name  string           `json:"name"`
		Steps []StepDefinition `json:"steps"`
	}{
		Name:  name,
		Steps: steps,
	})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(canonicalData)
	return hex.EncodeToString(hash[:]), nil
}

package rules

import (
	"crypto/sha256"
	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
)

func ComputeBundleHash(payload map[string]interface{}) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if payload == nil {
		return "", appErrors.NewValidationError("payload cannot be nil")
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", appErrors.NewInternalError("failed to serialize payload: " + err.Error())
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:]), nil
}

func VerifyBundleIntegrity(payload map[string]interface{}, expectedHash string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	computed, err := ComputeBundleHash(payload)
	if err != nil {
		return err
	}
	if !strings.EqualFold(computed, expectedHash) {
		return appErrors.NewConflictError(fmt.Sprintf("bundle hash mismatch: expected %s got %s", expectedHash, computed))
	}
	return nil
}

func ValidateMakerChecker(proposerID string, approverID string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	trimmedProposer := strings.TrimSpace(proposerID)
	trimmedApprover := strings.TrimSpace(approverID)
	if trimmedProposer == "" || trimmedApprover == "" {
		return appErrors.NewValidationError("proposer_id and approver_id are required")
	}
	if trimmedProposer == trimmedApprover {
		return appErrors.NewForbiddenError("dual authorization violation: approver cannot be the same as proposer")
	}
	return nil
}

func MatchWorkerCapabilities(required []string, actual []string) bool {
	defer observability.TraceEnter(nil)()
	if len(required) == 0 {
		return true
	}
	actualMap := make(map[string]struct{}, len(actual))
	for _, cap := range actual {
		actualMap[cap] = struct{}{}
	}
	for _, req := range required {
		if _, ok := actualMap[req]; !ok {
			return false
		}
	}
	return true
}

func ComputeEntityCohortBucket(entityID string) int {
	defer observability.TraceEnter(nil)()
	hasher := fnv.New32a()
	hasher.Write([]byte(entityID))
	return int(hasher.Sum32() % 100)
}

func SnapshotLiveInputs(inputs map[string]interface{}) map[string]interface{} {
	defer observability.TraceEnter(nil)()
	if inputs == nil {
		return make(map[string]interface{})
	}
	cloned := make(map[string]interface{}, len(inputs))
	for k, v := range inputs {
		cloned[k] = v
	}
	return cloned
}

func CanMigrateAtSafePoint(currentStep string, targetSafePoint string, targetStatus contracts.BundleStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if targetStatus != contracts.BundleStatusActive {
		return appErrors.NewPreconditionFailedError("target bundle must be ACTIVE for migration")
	}
	if strings.TrimSpace(currentStep) == "" || strings.TrimSpace(targetSafePoint) == "" {
		return appErrors.NewValidationError("step identifiers cannot be empty")
	}
	if currentStep != targetSafePoint {
		return appErrors.NewConflictError("saga has progressed past or not reached the declared migration safe point")
	}
	return nil
}

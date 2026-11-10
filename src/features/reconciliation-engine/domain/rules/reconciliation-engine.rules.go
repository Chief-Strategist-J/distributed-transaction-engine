package rules

import (
	contracts "distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"fmt"
	"strings"
)

type ReconciliationRules struct{}

func NewReconciliationRules() *ReconciliationRules {
	return &ReconciliationRules{}
}

func (r *ReconciliationRules) ClassifyDrift(input contracts.EvaluateReconciliationInput) contracts.ReconciliationEvaluationResult {
	defer observability.TraceEnter(nil)()
	return EvaluateDrift(input)
}

func EvaluateDrift(input contracts.EvaluateReconciliationInput) contracts.ReconciliationEvaluationResult {
	defer observability.TraceEnter(nil)()
	normInternal := strings.ToUpper(strings.TrimSpace(input.InternalStatus))
	normExternal := strings.ToUpper(strings.TrimSpace(input.ExternalStatus))

	if res, matched := checkGhostTransaction(normInternal, normExternal); matched {
		return res
	}
	if res, matched := checkPhantomSettlement(normInternal, normExternal); matched {
		return res
	}
	if normInternal == "COMPLETED" && normExternal == "COMMITTED" {
		return checkNumericalDrift(input.InternalAmountCents, input.ExternalAmountCents)
	}
	if normInternal != normExternal {
		return buildStatusMismatchResult(normInternal, normExternal)
	}
	return buildBalancedResult("State matched")
}

func checkGhostTransaction(internal, external string) (contracts.ReconciliationEvaluationResult, bool) {
	if internal == "COMPENSATED" && external == "COMMITTED" {
		anomaly := contracts.AnomalyGhostTransaction
		return contracts.ReconciliationEvaluationResult{
			IsAnomaly:         true,
			AnomalyType:       &anomaly,
			CanAutoRepair:     true,
			SuggestedAction:   "DISPATCH_DELAYED_REFUND",
			DiscrepancyReason: "Downstream committed transaction while engine state was compensated",
		}, true
	}
	return contracts.ReconciliationEvaluationResult{}, false
}

func checkPhantomSettlement(internal, external string) (contracts.ReconciliationEvaluationResult, bool) {
	if internal == "COMPLETED" && (external == "NOT_FOUND" || external == "FAILED") {
		anomaly := contracts.AnomalyPhantomSettlement
		return contracts.ReconciliationEvaluationResult{
			IsAnomaly:         true,
			AnomalyType:       &anomaly,
			CanAutoRepair:     true,
			SuggestedAction:   "TRIGGER_DOWNSTREAM_REPLAY",
			DiscrepancyReason: "Engine assumed completed settlement but downstream returned not found/failed",
		}, true
	}
	return contracts.ReconciliationEvaluationResult{}, false
}

func checkNumericalDrift(internal, external int64) contracts.ReconciliationEvaluationResult {
	drift := internal - external
	if drift < 0 {
		drift = -drift
	}
	if drift == 0 {
		return buildBalancedResult("Transaction balanced and consistent")
	}
	anomaly := contracts.AnomalyNumericalDrift
	canAutoRepair := drift <= 100
	action := "ESCALATE_MANUAL_REVIEW"
	if canAutoRepair {
		action = "DISPATCH_FEE_ADJUSTMENT"
	}
	return contracts.ReconciliationEvaluationResult{
		IsAnomaly:         true,
		AnomalyType:       &anomaly,
		CanAutoRepair:     canAutoRepair,
		SuggestedAction:   action,
		DiscrepancyReason: fmt.Sprintf("Numerical amount mismatch: internal %d cents vs external %d cents", internal, external),
	}
}

func buildStatusMismatchResult(internal, external string) contracts.ReconciliationEvaluationResult {
	anomaly := contracts.AnomalyStatusMismatch
	return contracts.ReconciliationEvaluationResult{
		IsAnomaly:         true,
		AnomalyType:       &anomaly,
		CanAutoRepair:     false,
		SuggestedAction:   "ESCALATE_MANUAL_REVIEW",
		DiscrepancyReason: fmt.Sprintf("Status disagreement: internal %s vs external %s", internal, external),
	}
}

func buildBalancedResult(reason string) contracts.ReconciliationEvaluationResult {
	return contracts.ReconciliationEvaluationResult{
		IsAnomaly:         false,
		AnomalyType:       nil,
		CanAutoRepair:     false,
		SuggestedAction:   "NOOP",
		DiscrepancyReason: reason,
	}
}

package rules

import (
	"fmt"
	"strings"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

type StepReconcileAction string

const (
	ActionScheduleForward        StepReconcileAction = "SCHEDULE_FORWARD"
	ActionRedispatchForward      StepReconcileAction = "REDISPATCH_FORWARD"
	ActionRedispatchCompensation StepReconcileAction = "REDISPATCH_COMPENSATION"
	ActionNoOp                   StepReconcileAction = "NO_OP"
)

func BuildIncarnationID(podName string, bootUUID string) string {
	cleanPod := strings.TrimSpace(podName)
	if cleanPod == "" {
		cleanPod = "worker"
	}
	cleanBoot := strings.TrimSpace(bootUUID)
	if cleanBoot == "" {
		cleanBoot = "default"
	}
	return fmt.Sprintf("%s_%s", cleanPod, cleanBoot)
}

func IsPoisonPill(takeoverCount int, maxTakeovers int) bool {
	return takeoverCount > maxTakeovers
}

func ClassifyStepReconciliation(state sagaContracts.StepExecutionState) StepReconcileAction {
	switch state {
	case sagaContracts.StepStatePending:
		return ActionScheduleForward
	case sagaContracts.StepStateDispatched, sagaContracts.StepStateUnknown:
		return ActionRedispatchForward
	case sagaContracts.StepStateRetryWait:
		return ActionRedispatchForward
	case sagaContracts.StepStateCompensating:
		return ActionRedispatchCompensation
	default:
		return ActionNoOp
	}
}

func ShouldSelfFence(now time.Time, lastRenewal time.Time, leaseDuration time.Duration, margin time.Duration) bool {
	fenceDeadline := lastRenewal.Add(leaseDuration - margin)
	return !now.Before(fenceDeadline)
}

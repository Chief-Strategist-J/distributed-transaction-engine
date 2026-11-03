package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/crash-recovery/domain/rules"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

func TestIncarnationIDGeneration(t *testing.T) {
	testCases := []struct {
		name     string
		podName  string
		bootUUID string
		expected string
	}{
		{
			name:     "standard pod and uuid",
			podName:  "engine-pod-0",
			bootUUID: "uuid-1234",
			expected: "engine-pod-0_uuid-1234",
		},
		{
			name:     "empty pod name fallback",
			podName:  "",
			bootUUID: "uuid-5678",
			expected: "worker_uuid-5678",
		},
		{
			name:     "empty uuid fallback",
			podName:  "worker-1",
			bootUUID: "",
			expected: "worker-1_default",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := rules.BuildIncarnationID(tc.podName, tc.bootUUID)
			if res != tc.expected {
				t.Fatalf("expected %s, got %s", tc.expected, res)
			}
		})
	}
}

func TestPoisonPillEvaluation(t *testing.T) {
	testCases := []struct {
		name          string
		takeoverCount int
		maxTakeovers  int
		expected      bool
	}{
		{
			name:          "below threshold",
			takeoverCount: 3,
			maxTakeovers:  5,
			expected:      false,
		},
		{
			name:          "at threshold",
			takeoverCount: 5,
			maxTakeovers:  5,
			expected:      false,
		},
		{
			name:          "exceeded threshold",
			takeoverCount: 6,
			maxTakeovers:  5,
			expected:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := rules.IsPoisonPill(tc.takeoverCount, tc.maxTakeovers)
			if res != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, res)
			}
		})
	}
}

func TestStepReconciliationClassification(t *testing.T) {
	testCases := []struct {
		name     string
		state    sagaContracts.StepExecutionState
		expected rules.StepReconcileAction
	}{
		{
			name:     "pending step never queried downstream",
			state:    sagaContracts.StepStatePending,
			expected: rules.ActionScheduleForward,
		},
		{
			name:     "dispatched step treated as ambiguous redispatch",
			state:    sagaContracts.StepStateDispatched,
			expected: rules.ActionRedispatchForward,
		},
		{
			name:     "unknown step needs redispatch",
			state:    sagaContracts.StepStateUnknown,
			expected: rules.ActionRedispatchForward,
		},
		{
			name:     "compensating step needs compensation redispatch",
			state:    sagaContracts.StepStateCompensating,
			expected: rules.ActionRedispatchCompensation,
		},
		{
			name:     "succeeded step is no-op",
			state:    sagaContracts.StepStateSucceeded,
			expected: rules.ActionNoOp,
		},
		{
			name:     "compensated step is no-op",
			state:    sagaContracts.StepStateCompensated,
			expected: rules.ActionNoOp,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := rules.ClassifyStepReconciliation(tc.state)
			if res != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, res)
			}
		})
	}
}

func TestSelfFenceEvaluation(t *testing.T) {
	now := time.Now()
	leaseDuration := 30 * time.Second
	margin := 5 * time.Second

	testCases := []struct {
		name        string
		currentTime time.Time
		lastRenewal time.Time
		expected    bool
	}{
		{
			name:        "well within lease window",
			currentTime: now,
			lastRenewal: now.Add(-10 * time.Second),
			expected:    false,
		},
		{
			name:        "at self-fence margin boundary (25s elapsed)",
			currentTime: now,
			lastRenewal: now.Add(-25 * time.Second),
			expected:    true,
		},
		{
			name:        "past lease expiration",
			currentTime: now,
			lastRenewal: now.Add(-35 * time.Second),
			expected:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := rules.ShouldSelfFence(tc.currentTime, tc.lastRenewal, leaseDuration, margin)
			if res != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, res)
			}
		})
	}
}


package tests

import (
	"testing"
	"time"

	"distributed-transaction-engine/src/features/crash-recovery/contracts"
)

func TestCrashRecoveryConfigValidation(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         contracts.CrashRecoveryConfig
		expectValid bool
	}{
		{
			name: "valid configuration",
			cfg: contracts.CrashRecoveryConfig{
				IncarnationID:      "worker-1_uuid-1234",
				ScanInterval:       5 * time.Second,
				BatchSize:          50,
				LeaseDuration:      30 * time.Second,
				MaxTakeovers:       5,
				HeartbeatInterval:  10 * time.Second,
				SelfFenceMargin:    5 * time.Second,
				MaxConcurrentOwned: 100,
			},
			expectValid: true,
		},
		{
			name: "empty incarnation id",
			cfg: contracts.CrashRecoveryConfig{
				IncarnationID:   "",
				ScanInterval:    5 * time.Second,
				BatchSize:       50,
				LeaseDuration:   30 * time.Second,
				MaxTakeovers:    5,
				SelfFenceMargin: 5 * time.Second,
			},
			expectValid: false,
		},
		{
			name: "scan interval too short",
			cfg: contracts.CrashRecoveryConfig{
				IncarnationID:   "worker-1_uuid",
				ScanInterval:    10 * time.Millisecond,
				BatchSize:       50,
				LeaseDuration:   30 * time.Second,
				MaxTakeovers:    5,
				SelfFenceMargin: 5 * time.Second,
			},
			expectValid: false,
		},
		{
			name: "self fence margin exceeds lease duration",
			cfg: contracts.CrashRecoveryConfig{
				IncarnationID:   "worker-1_uuid",
				ScanInterval:    5 * time.Second,
				BatchSize:       50,
				LeaseDuration:   30 * time.Second,
				MaxTakeovers:    5,
				SelfFenceMargin: 35 * time.Second,
			},
			expectValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := contracts.ValidateCrashRecoveryConfig(tc.cfg)
			if tc.expectValid && err != nil {
				t.Fatalf("expected valid config, got error: %v", err)
			}
			if !tc.expectValid && err == nil {
				t.Fatalf("expected validation error, got nil")
			}
		})
	}
}


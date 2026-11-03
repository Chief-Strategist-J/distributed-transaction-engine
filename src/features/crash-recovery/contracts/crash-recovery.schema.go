package contracts

import (
	"strings"
	"time"

	"distributed-transaction-engine/src/shared/errors"
)

func ValidateCrashRecoveryConfig(cfg CrashRecoveryConfig) error {
	if strings.TrimSpace(cfg.IncarnationID) == "" {
		return errors.NewValidationError("incarnationId cannot be empty")
	}
	if cfg.ScanInterval < 100*time.Millisecond {
		return errors.NewValidationError("scanInterval must be at least 100ms")
	}
	if cfg.BatchSize < 1 || cfg.BatchSize > 1000 {
		return errors.NewValidationError("batchSize must be between 1 and 1000")
	}
	if cfg.LeaseDuration < 1*time.Second {
		return errors.NewValidationError("leaseDuration must be at least 1s")
	}
	if cfg.MaxTakeovers < 1 {
		return errors.NewValidationError("maxTakeovers must be greater than or equal to 1")
	}
	if cfg.SelfFenceMargin <= 0 || cfg.SelfFenceMargin >= cfg.LeaseDuration {
		return errors.NewValidationError("selfFenceMargin must be positive and less than leaseDuration")
	}
	return nil
}

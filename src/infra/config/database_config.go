package config

import (
	"errors"
	"os"
	"strings"
)

func (d *DatabaseConfig) ApplyEnvironment() {
	if enforced := strings.ToLower(strings.TrimSpace(os.Getenv("DB_RLS_ENFORCED"))); enforced == "true" || enforced == "false" {
		d.RLSEnforced = enforced == "true"
	}
	if auto := strings.ToLower(strings.TrimSpace(os.Getenv("AUTO_MIGRATE"))); auto == "true" || auto == "false" {
		value := auto == "true"
		d.AutoMigrate = &value
	}
}

func (d *DatabaseConfig) MigrateOnStart(environment string) bool {
	if d.AutoMigrate != nil {
		return *d.AutoMigrate
	}
	return !secureEnvironments[strings.ToLower(strings.TrimSpace(environment))]
}

func (d *DatabaseConfig) Validate(environment string) error {
	if !secureEnvironments[strings.ToLower(strings.TrimSpace(environment))] {
		return nil
	}
	if strings.TrimSpace(d.Host) == "" || d.Password == "" {
		return errors.New("DB_HOST and DB_PASSWORD (or DB_PASSWORD_FILE) are required in production and staging")
	}
	if strings.EqualFold(strings.TrimSpace(d.User), "postgres") {
		return errors.New("production and staging must connect with the restricted login role, not the postgres superuser")
	}
	if !d.RLSEnforced {
		return errors.New("DB_RLS_ENFORCED must be true in production and staging")
	}
	if d.AutoMigrate != nil && *d.AutoMigrate {
		return errors.New("AUTO_MIGRATE must not be enabled in production and staging; run the migrate command as a separate job")
	}
	return nil
}

package config

import (
	"errors"
	"os"
	"strings"
)

func (d *DatabaseConfig) ApplyEnvironment() {
	if root := strings.TrimSpace(os.Getenv("DB_SSLROOTCERT")); root != "" {
		d.SSLRootCert = root
	}
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
	if err := d.validateTransportSecurity(); err != nil {
		return err
	}
	if !d.RLSEnforced {
		return errors.New("DB_RLS_ENFORCED must be true in production and staging")
	}
	if d.AutoMigrate != nil && *d.AutoMigrate {
		return errors.New("AUTO_MIGRATE must not be enabled in production and staging; run the migrate command as a separate job")
	}
	return nil
}

func (d *DatabaseConfig) validateTransportSecurity() error {
	mode := strings.ToLower(strings.TrimSpace(d.SSLMode))
	if mode != "verify-full" && mode != "verify-ca" {
		return errors.New("DB_SSLMODE must be verify-full or verify-ca in production and staging so the server identity is verified")
	}
	if strings.TrimSpace(d.SSLRootCert) == "" {
		return errors.New("DB_SSLROOTCERT is required in production and staging")
	}
	return nil
}

func (d *DatabaseConfig) DSN() string {
	pairs := [][2]string{
		{"host", d.Host}, {"port", d.Port}, {"user", d.User}, {"password", d.Password},
		{"dbname", d.Name}, {"sslmode", d.SSLMode},
	}
	if d.SSLRootCert != "" {
		pairs = append(pairs, [2]string{"sslrootcert", d.SSLRootCert})
	}
	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		parts = append(parts, pair[0]+"="+quoteDSNValue(pair[1]))
	}
	return strings.Join(parts, " ")
}

func quoteDSNValue(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value)
	return "'" + escaped + "'"
}

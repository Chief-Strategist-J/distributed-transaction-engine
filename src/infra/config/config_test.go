package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

var ambientVariables = []string{
	"ENV", "PORT", "ADMIN_PORT", "DB_HOST", "DB_PORT", "DB_USER", "DB_NAME", "DB_SSLMODE", "DB_PASSWORD", "DB_PASSWORD_FILE",
	"DB_MAX_OPEN_CONNS", "DB_MAX_IDLE_CONNS", "DB_CONN_MAX_LIFETIME", "DB_RLS_ENFORCED", "AUTO_MIGRATE", "MIGRATIONS_DIR",
	"SHUTDOWN_DEADLINE_SECONDS", "MAX_REQUEST_BODY_BYTES", "EXPOSE_ERROR_DETAILS", "WORKER_ID", "POLLER_ENABLED",
	"POLLER_LEASE_DURATION_SECONDS", "POLLER_INTERVAL_MILLIS", "KAFKA_BROKERS", "KAFKA_WRITE_TIMEOUT_MS", "KAFKA_BATCH_TIMEOUT_MS",
	"KAFKA_REPLICATION_FACTOR", "KAFKA_TOPICS_MANIFEST", "KAFKA_TLS_ENABLED", "KAFKA_CA_FILE", "KAFKA_SASL_MECHANISM", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "KAFKA_SASL_PASSWORD_FILE", "DB_SSLROOTCERT", "KAFKA_AUTO_CREATE_TOPICS", "OUTBOX_RELAY_ENABLED",
	"OUTBOX_RELAY_INTERVAL_MILLIS", "OUTBOX_RELAY_BATCH_SIZE", "INBOX_PRUNE_ENABLED", "INBOX_RETENTION_DAYS", "INBOX_PRUNE_INTERVAL_MINUTES", "INBOX_PRUNE_BATCH_SIZE", "AUTH_MODE", "AUTH_JWKS_FILE", "AUTH_ISSUER", "AUTH_AUDIENCE",
	"AUTH_LEEWAY_SECONDS",
}

func TestMain(m *testing.M) {
	for _, name := range ambientVariables {
		_ = os.Unsetenv(name)
	}
	os.Exit(m.Run())
}

func TestConfig_LoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error loading defaults: %v", err)
	}

	if cfg.Server.BindAddress != ":8080" {
		t.Errorf("expected default bind address :8080, got %s", cfg.Server.BindAddress)
	}
	if cfg.Database.Port != "5432" {
		t.Errorf("expected default db port 5432, got %s", cfg.Database.Port)
	}
	if cfg.Database.Name != "dist_tx_engine" {
		t.Errorf("expected default db name dist_tx_engine, got %s", cfg.Database.Name)
	}
	if cfg.ReadTimeout() != 15*time.Second {
		t.Errorf("expected 15s read timeout, got %v", cfg.ReadTimeout())
	}
}

func TestConfig_LoadFromYAML(t *testing.T) {
	yamlContent := `
server:
  bind_address: ":9999"
  admin_bind_address: ":9998"
  read_timeout_seconds: 30
database:
  host: "custom-db"
  name: "custom_tx"
  max_open_conns: 50
poller:
  worker_id: "worker-test"
  lease_duration_seconds: 45
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test.yaml")
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("unexpected error loading yaml: %v", err)
	}

	if cfg.Server.BindAddress != ":9999" {
		t.Errorf("expected :9999, got %s", cfg.Server.BindAddress)
	}
	if cfg.Database.Host != "custom-db" {
		t.Errorf("expected custom-db, got %s", cfg.Database.Host)
	}
	if cfg.Database.MaxOpenConns != 50 {
		t.Errorf("expected 50 max open conns, got %d", cfg.Database.MaxOpenConns)
	}
	if cfg.Poller.WorkerID != "worker-test" {
		t.Errorf("expected worker-test, got %s", cfg.Poller.WorkerID)
	}
	if cfg.LeaseDuration() != 45*time.Second {
		t.Errorf("expected 45s lease duration, got %v", cfg.LeaseDuration())
	}
}

func TestConfig_EnvOverrides(t *testing.T) {
	os.Setenv("PORT", "7777")
	os.Setenv("ADMIN_PORT", "7778")
	os.Setenv("DB_HOST", "env-host")
	os.Setenv("DB_PASSWORD", "env-secret")
	os.Setenv("WORKER_ID", "env-worker")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("ADMIN_PORT")
		os.Unsetenv("DB_HOST")
		os.Unsetenv("DB_PASSWORD")
		os.Unsetenv("WORKER_ID")
	}()

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.BindAddress != ":7777" {
		t.Errorf("expected :7777, got %s", cfg.Server.BindAddress)
	}
	if cfg.Server.AdminBindAddress != ":7778" {
		t.Errorf("expected :7778, got %s", cfg.Server.AdminBindAddress)
	}
	if cfg.Database.Host != "env-host" {
		t.Errorf("expected env-host, got %s", cfg.Database.Host)
	}
	if cfg.Database.Password != "env-secret" {
		t.Errorf("expected env-secret, got %s", cfg.Database.Password)
	}
	if cfg.Poller.WorkerID != "env-worker" {
		t.Errorf("expected env-worker, got %s", cfg.Poller.WorkerID)
	}
}

func TestConfig_PasswordFileOverride(t *testing.T) {
	tmpDir := t.TempDir()
	secretPath := filepath.Join(tmpDir, "secret.txt")
	if err := os.WriteFile(secretPath, []byte("file-password\n"), 0644); err != nil {
		t.Fatalf("failed to write secret file: %v", err)
	}

	os.Setenv("DB_PASSWORD_FILE", secretPath)
	defer os.Unsetenv("DB_PASSWORD_FILE")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Database.Password != "file-password" {
		t.Errorf("expected file-password, got %s", cfg.Database.Password)
	}
}

func TestPollerCanBeDisabledFromTheEnvironment(t *testing.T) {
	t.Setenv("POLLER_ENABLED", "false")
	cfg, err := Load("")
	if err != nil {
		t.Skipf("config load needs the default file: %v", err)
	}
	if !cfg.Poller.Disabled {
		t.Fatal("POLLER_ENABLED=false must disable the poller")
	}
	t.Setenv("POLLER_ENABLED", "true")
	if enabled, _ := Load(""); enabled != nil && enabled.Poller.Disabled {
		t.Fatal("POLLER_ENABLED=true must keep the poller on")
	}
}

func TestOperationalDefaultsAndEnvironmentOverrides(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Skipf("config load needs the default file: %v", err)
	}
	if cfg.ShutdownDeadline() < 35*time.Second || cfg.Database.MaxOpenConns != 50 || cfg.Database.MaxIdleConns != 10 || cfg.MaxBodyLimit() != 10<<20 {
		t.Fatalf("production defaults missing: deadline=%v open=%d idle=%d body=%d", cfg.ShutdownDeadline(), cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns, cfg.MaxBodyLimit())
	}
	t.Setenv("DB_MAX_OPEN_CONNS", "80")
	t.Setenv("DB_MAX_IDLE_CONNS", "20")
	t.Setenv("DB_CONN_MAX_LIFETIME", "12m")
	t.Setenv("SHUTDOWN_DEADLINE_SECONDS", "50")
	t.Setenv("MAX_REQUEST_BODY_BYTES", "2048")
	tuned, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if tuned.Database.MaxOpenConns != 80 || tuned.Database.MaxIdleConns != 20 || tuned.ConnMaxLifetime() != 12*time.Minute || tuned.ShutdownDeadline() != 50*time.Second || tuned.MaxBodyLimit() != 2048 {
		t.Fatalf("environment overrides not applied: %+v", tuned.Database)
	}
}

func TestAuthConfig_ValidateMatrix(t *testing.T) {
	complete := AuthConfig{Mode: "jwt", JWKSFile: "/keys/jwks.json", Issuer: "https://issuer", Audience: "dtx"}
	cases := []struct {
		name        string
		cfg         AuthConfig
		environment string
		wantErr     bool
		wantMode    string
	}{
		{"dev_default_disabled", AuthConfig{}, "", false, "disabled"},
		{"dev_explicit_jwt_complete", complete, "development", false, "jwt"},
		{"dev_jwt_missing_jwks", AuthConfig{Mode: "jwt", Issuer: "i", Audience: "a"}, "", true, "jwt"},
		{"dev_jwt_missing_issuer", AuthConfig{Mode: "jwt", JWKSFile: "f", Audience: "a"}, "", true, "jwt"},
		{"dev_jwt_missing_audience", AuthConfig{Mode: "jwt", JWKSFile: "f", Issuer: "i"}, "", true, "jwt"},
		{"production_default_requires_jwt", AuthConfig{}, "production", true, "jwt"},
		{"production_complete", complete, "production", false, "jwt"},
		{"production_cannot_disable", AuthConfig{Mode: "disabled"}, "production", true, "disabled"},
		{"staging_cannot_disable", AuthConfig{Mode: "DISABLED"}, " Staging ", true, "disabled"},
		{"production_case_insensitive", complete, "PRODUCTION", false, "jwt"},
		{"unknown_mode", AuthConfig{Mode: "basic"}, "", true, "basic"},
		{"leeway_too_large", AuthConfig{Mode: "jwt", JWKSFile: "f", Issuer: "i", Audience: "a", LeewaySeconds: 301}, "", true, "jwt"},
		{"leeway_negative", AuthConfig{Mode: "jwt", JWKSFile: "f", Issuer: "i", Audience: "a", LeewaySeconds: -1}, "", true, "jwt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.ResolvedMode(tc.environment); got != tc.wantMode {
				t.Fatalf("expected mode %q got %q", tc.wantMode, got)
			}
			if err := tc.cfg.Validate(tc.environment); (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got %v", tc.wantErr, err)
			}
		})
	}
}

func TestAuthConfig_ProductionLoadFailsWithoutKeys(t *testing.T) {
	t.Setenv("ENV", "production")
	if _, err := Load(""); err == nil {
		t.Fatal("production must refuse to boot without jwt configuration")
	}
	t.Setenv("AUTH_MODE", "disabled")
	if _, err := Load(""); err == nil {
		t.Fatal("production must refuse to boot with auth disabled")
	}
}

func TestAuthConfig_EnvironmentOverridesAndLoads(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("AUTH_JWKS_FILE", "/keys/jwks.json")
	t.Setenv("AUTH_ISSUER", "https://issuer")
	t.Setenv("AUTH_AUDIENCE", "dtx")
	t.Setenv("AUTH_LEEWAY_SECONDS", "10")
	t.Setenv("DB_RLS_ENFORCED", "true")
	t.Setenv("DB_HOST", "db.internal")
	t.Setenv("DB_USER", "dtx_login")
	t.Setenv("DB_PASSWORD", "secret")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("complete production config must load: %v", err)
	}
	if cfg.Auth.JWKSFile != "/keys/jwks.json" || cfg.Auth.Leeway() != 10*time.Second {
		t.Fatalf("env overrides not applied: %#v", cfg.Auth)
	}
}

func TestDatabaseConfig_ProductionRules(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name        string
		cfg         DatabaseConfig
		environment string
		wantErr     bool
		wantMigrate bool
	}{
		{"dev_defaults", DatabaseConfig{}, "", false, true},
		{"dev_explicit_no_migrate", DatabaseConfig{AutoMigrate: &no}, "development", false, false},
		{"production_requires_rls", DatabaseConfig{Host: "h", User: "dtx_login", Password: "p", SSLMode: "verify-full", SSLRootCert: "/ca.crt"}, "production", true, false},
		{"production_rls_ok", productionDB(true), "production", false, false},
		{"production_auto_migrate_refused", withAutoMigrate(productionDB(true), &yes), "production", true, true},
		{"staging_requires_rls", productionDB(false), " Staging ", true, false},
		{"production_case_insensitive", productionDB(true), "PRODUCTION", false, false},
		{"production_requires_host", DatabaseConfig{User: "dtx_login", Password: "p", RLSEnforced: true, SSLMode: "verify-full", SSLRootCert: "/ca.crt"}, "production", true, false},
		{"production_requires_password", DatabaseConfig{Host: "h", User: "dtx_login", RLSEnforced: true, SSLMode: "verify-full", SSLRootCert: "/ca.crt"}, "production", true, false},
		{"production_refuses_superuser", DatabaseConfig{Host: "h", User: "Postgres", Password: "p", RLSEnforced: true, SSLMode: "verify-full", SSLRootCert: "/ca.crt"}, "production", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(tc.environment); (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got %v", tc.wantErr, err)
			}
			if got := tc.cfg.MigrateOnStart(tc.environment); got != tc.wantMigrate {
				t.Fatalf("MigrateOnStart expected %v got %v", tc.wantMigrate, got)
			}
		})
	}
}

func TestDatabaseConfig_EnvironmentOverrides(t *testing.T) {
	t.Setenv("DB_RLS_ENFORCED", "true")
	t.Setenv("AUTO_MIGRATE", "false")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Database.RLSEnforced || cfg.Database.AutoMigrate == nil || *cfg.Database.AutoMigrate {
		t.Fatalf("env overrides not applied: %#v", cfg.Database)
	}
}

func productionDB(rls bool) DatabaseConfig {
	return DatabaseConfig{Host: "db.internal", User: "dtx_login", Password: "secret", RLSEnforced: rls, SSLMode: "verify-full", SSLRootCert: "/ca.crt"}
}

func withAutoMigrate(cfg DatabaseConfig, value *bool) DatabaseConfig {
	cfg.AutoMigrate = value
	return cfg
}

func TestKafkaConfig_ProductionRules(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name        string
		cfg         KafkaConfig
		brokers     string
		environment string
		wantErr     bool
		wantCreate  bool
	}{
		{"dev_defaults_create_topics", KafkaConfig{}, "", "", false, true},
		{"dev_explicit_off", KafkaConfig{AutoCreateTopics: &no}, "", "development", false, false},
		{"production_requires_brokers", KafkaConfig{}, "", "production", true, false},
		{"production_brokers_ok", secureKafka(), "kafka:9092", "production", false, false},
		{"production_refuses_auto_create", withAutoCreate(secureKafka(), &yes), "kafka:9092", "production", true, true},
		{"production_requires_tls", KafkaConfig{SASLMechanism: "SCRAM-SHA-512", SASLUsername: "u", SASLPassword: "p"}, "kafka:9092", "production", true, false},
		{"production_requires_sasl", KafkaConfig{TLSEnabled: true}, "kafka:9092", "production", true, false},
		{"production_rejects_unknown_mechanism", withMechanism(secureKafka(), "GSSAPI"), "kafka:9092", "production", true, false},
		{"production_requires_credentials", KafkaConfig{TLSEnabled: true, SASLMechanism: "PLAIN"}, "kafka:9092", "production", true, false},
		{"staging_requires_brokers", KafkaConfig{}, "  ", "Staging", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(tc.environment, tc.brokers); (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got %v", tc.wantErr, err)
			}
			if got := tc.cfg.CreateTopicsOnStart(tc.environment); got != tc.wantCreate {
				t.Fatalf("CreateTopicsOnStart expected %v got %v", tc.wantCreate, got)
			}
		})
	}
}

func TestKafkaAndOutboxConfig_EnvironmentOverridesAndDefaults(t *testing.T) {
	defaults, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Outbox.RelayInterval() != 500*time.Millisecond || defaults.Outbox.BatchSize() != 100 || defaults.Kafka.Manifest() != "messaging/topics/topics.yaml" {
		t.Fatalf("unexpected defaults: %#v %#v", defaults.Outbox, defaults.Kafka)
	}
	t.Setenv("KAFKA_WRITE_TIMEOUT_MS", "2500")
	t.Setenv("KAFKA_BATCH_TIMEOUT_MS", "20")
	t.Setenv("KAFKA_REPLICATION_FACTOR", "3")
	t.Setenv("KAFKA_TOPICS_MANIFEST", "/etc/topics.yaml")
	t.Setenv("OUTBOX_RELAY_ENABLED", "false")
	t.Setenv("OUTBOX_RELAY_INTERVAL_MILLIS", "1500")
	t.Setenv("OUTBOX_RELAY_BATCH_SIZE", "25")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.WriteTimeout() != 2500*time.Millisecond || cfg.Kafka.BatchTimeout() != 20*time.Millisecond || cfg.Kafka.ReplicationFactor != 3 || cfg.Kafka.Manifest() != "/etc/topics.yaml" {
		t.Fatalf("kafka overrides not applied: %#v", cfg.Kafka)
	}
	if !cfg.Outbox.RelayDisabled || cfg.Outbox.RelayInterval() != 1500*time.Millisecond || cfg.Outbox.BatchSize() != 25 {
		t.Fatalf("outbox overrides not applied: %#v", cfg.Outbox)
	}
}

func TestKafkaAndOutboxConfig_InvalidOverridesFallBackToDefaults(t *testing.T) {
	t.Setenv("KAFKA_WRITE_TIMEOUT_MS", "-5")
	t.Setenv("OUTBOX_RELAY_INTERVAL_MILLIS", "abc")
	t.Setenv("OUTBOX_RELAY_BATCH_SIZE", "0")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.WriteTimeoutMillis != 0 || cfg.Outbox.RelayInterval() != 500*time.Millisecond || cfg.Outbox.BatchSize() != 100 {
		t.Fatalf("invalid values must be ignored: %#v %#v", cfg.Kafka, cfg.Outbox)
	}
}

func TestDatabaseDSN_QuotesEveryValueSoCredentialsCannotInjectParameters(t *testing.T) {
	cfg := DatabaseConfig{Host: "db.internal", Port: "5432", User: "dtx", Password: `p w'd\x sslmode=disable`, Name: "dist_tx_engine", SSLMode: "verify-full", SSLRootCert: "/etc/ca.crt"}
	want := `host='db.internal' port='5432' user='dtx' password='p w\'d\\x sslmode=disable' dbname='dist_tx_engine' sslmode='verify-full' sslrootcert='/etc/ca.crt'`
	if got := cfg.DSN(); got != want {
		t.Fatalf("unexpected DSN:\n got %s\nwant %s", got, want)
	}
}

func TestDatabaseConfig_ProductionRequiresVerifiedTLS(t *testing.T) {
	good := DatabaseConfig{Host: "h", User: "dtx_login", Password: "p", RLSEnforced: true, SSLMode: "verify-full", SSLRootCert: "/ca.crt"}
	cases := map[string]func(*DatabaseConfig){
		"disable":         func(d *DatabaseConfig) { d.SSLMode = "disable" },
		"empty":           func(d *DatabaseConfig) { d.SSLMode = "" },
		"require_only":    func(d *DatabaseConfig) { d.SSLMode = "require" },
		"prefer":          func(d *DatabaseConfig) { d.SSLMode = "prefer" },
		"missing_root":    func(d *DatabaseConfig) { d.SSLRootCert = " " },
		"verify_ca_no_ca": func(d *DatabaseConfig) { d.SSLMode = "verify-ca"; d.SSLRootCert = "" },
	}
	if err := good.Validate("production"); err != nil {
		t.Fatalf("verified TLS must be accepted: %v", err)
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := good
			mutate(&candidate)
			if candidate.Validate("production") == nil {
				t.Fatalf("%s must be rejected in production", name)
			}
		})
	}
	insecure := DatabaseConfig{SSLMode: "disable"}
	if err := insecure.Validate("development"); err != nil {
		t.Fatalf("development must stay usable without TLS: %v", err)
	}
}

func secureKafka() KafkaConfig {
	return KafkaConfig{TLSEnabled: true, SASLMechanism: "SCRAM-SHA-512", SASLUsername: "dtx", SASLPassword: "secret"}
}

func withAutoCreate(cfg KafkaConfig, value *bool) KafkaConfig {
	cfg.AutoCreateTopics = value
	return cfg
}

func withMechanism(cfg KafkaConfig, mechanism string) KafkaConfig {
	cfg.SASLMechanism = mechanism
	return cfg
}

func TestKafkaSecurityEnvironment_AppliesFlagsAndSecrets(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "kafka-password")
	if err := os.WriteFile(passwordFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	t.Setenv("KAFKA_CA_FILE", "/etc/kafka/ca.pem")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USERNAME", "dtx")
	t.Setenv("KAFKA_SASL_PASSWORD_FILE", passwordFile)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Kafka.TLSEnabled || cfg.Kafka.CAFile != "/etc/kafka/ca.pem" || cfg.Kafka.SASLMechanism != "SCRAM-SHA-512" || cfg.Kafka.SASLUsername != "dtx" || cfg.Kafka.SASLPassword != "from-file" {
		t.Fatalf("security settings not applied: %#v", cfg.Kafka)
	}
	t.Setenv("KAFKA_SASL_PASSWORD", "direct")
	cfg, _ = Load("")
	if cfg.Kafka.SASLPassword != "direct" {
		t.Fatalf("an explicit password must win over the file, got %q", cfg.Kafka.SASLPassword)
	}
}

func TestInboxConfig_DefaultsAndOverrides(t *testing.T) {
	defaults, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Inbox.Retention() != 30*24*time.Hour || defaults.Inbox.PruneInterval() != time.Hour || defaults.Inbox.BatchSize() != 5000 || defaults.Inbox.PruneDisabled {
		t.Fatalf("unexpected inbox defaults: %#v", defaults.Inbox)
	}
	t.Setenv("INBOX_RETENTION_DAYS", "45")
	t.Setenv("INBOX_PRUNE_INTERVAL_MINUTES", "15")
	t.Setenv("INBOX_PRUNE_BATCH_SIZE", "250")
	t.Setenv("INBOX_PRUNE_ENABLED", "false")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Inbox.Retention() != 45*24*time.Hour || cfg.Inbox.PruneInterval() != 15*time.Minute || cfg.Inbox.BatchSize() != 250 || !cfg.Inbox.PruneDisabled {
		t.Fatalf("overrides not applied: %#v", cfg.Inbox)
	}
}

func TestInboxConfig_InvalidOverridesKeepSafeDefaults(t *testing.T) {
	t.Setenv("INBOX_RETENTION_DAYS", "0")
	t.Setenv("INBOX_PRUNE_BATCH_SIZE", "-4")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Inbox.Retention() != 30*24*time.Hour || cfg.Inbox.BatchSize() != 5000 {
		t.Fatalf("a non-positive retention or batch must fall back to the safe default: %#v", cfg.Inbox)
	}
}

func TestAuthConfig_DefaultClockSkewToleranceIsOneMinute(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.Leeway() != time.Minute {
		t.Fatalf("expected a 1 minute default clock skew tolerance, got %v", cfg.Auth.Leeway())
	}
	t.Setenv("AUTH_LEEWAY_SECONDS", "0")
	strict, _ := Load("")
	if strict.Auth.Leeway() != 0 {
		t.Fatalf("an explicit zero must disable the tolerance, got %v", strict.Auth.Leeway())
	}
}

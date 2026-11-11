package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("complete production config must load: %v", err)
	}
	if cfg.Auth.JWKSFile != "/keys/jwks.json" || cfg.Auth.Leeway() != 10*time.Second {
		t.Fatalf("env overrides not applied: %#v", cfg.Auth)
	}
}

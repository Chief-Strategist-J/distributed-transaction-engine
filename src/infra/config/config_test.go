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


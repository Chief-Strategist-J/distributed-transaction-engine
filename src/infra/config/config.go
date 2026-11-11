package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Name                    string `yaml:"name"`
	BindAddress             string `yaml:"bind_address"`
	AdminBindAddress        string `yaml:"admin_bind_address"`
	ReadTimeoutSeconds      int    `yaml:"read_timeout_seconds"`
	WriteTimeoutSeconds     int    `yaml:"write_timeout_seconds"`
	IdleTimeoutSeconds      int    `yaml:"idle_timeout_seconds"`
	ShutdownDeadlineSeconds int    `yaml:"shutdown_deadline_seconds"`
	MaxBodyBytes            int64  `yaml:"max_body_bytes"`
	ExposeErrorDetails      bool   `yaml:"expose_error_details"`
}

type DatabaseConfig struct {
	Host                   string `yaml:"host"`
	Port                   string `yaml:"port"`
	User                   string `yaml:"user"`
	Password               string `yaml:"password"`
	Name                   string `yaml:"name"`
	SSLMode                string `yaml:"sslmode"`
	SSLRootCert            string `yaml:"sslrootcert"`
	MaxOpenConns           int    `yaml:"max_open_conns"`
	MaxIdleConns           int    `yaml:"max_idle_conns"`
	ConnMaxLifetimeMinutes int    `yaml:"conn_max_lifetime_minutes"`
	MigrationsDir          string `yaml:"migrations_dir"`
	RLSEnforced            bool   `yaml:"rls_enforced"`
	AutoMigrate            *bool  `yaml:"auto_migrate"`
}

type PollerConfig struct {
	WorkerID             string `yaml:"worker_id"`
	LeaseDurationSeconds int    `yaml:"lease_duration_seconds"`
	PollIntervalMillis   int    `yaml:"poll_interval_millis"`
	Disabled             bool   `yaml:"disabled"`
}

type KafkaConfig struct {
	Brokers            string `yaml:"brokers"`
	WriteTimeoutMillis int    `yaml:"write_timeout_millis"`
	BatchTimeoutMillis int    `yaml:"batch_timeout_millis"`
	AutoCreateTopics   *bool  `yaml:"auto_create_topics"`
	TopicsManifest     string `yaml:"topics_manifest"`
	ReplicationFactor  int    `yaml:"replication_factor"`
	TLSEnabled         bool   `yaml:"tls_enabled"`
	CAFile             string `yaml:"ca_file"`
	SASLMechanism      string `yaml:"sasl_mechanism"`
	SASLUsername       string `yaml:"sasl_username"`
	SASLPassword       string `yaml:"sasl_password"`
}

type OutboxConfig struct {
	RelayDisabled       bool `yaml:"relay_disabled"`
	RelayIntervalMillis int  `yaml:"relay_interval_millis"`
	RelayBatchSize      int  `yaml:"relay_batch_size"`
}

type EndpointsConfig struct {
	BasePath      string `yaml:"base_path"`
	HealthLive    string `yaml:"health_live"`
	HealthReady   string `yaml:"health_ready"`
	Sagas         string `yaml:"sagas"`
	Workflows     string `yaml:"workflows"`
	State         string `yaml:"state"`
	CrashRecovery string `yaml:"crash_recovery"`
}

const DefaultTokenLeewaySeconds = 60

type AuthConfig struct {
	Mode          string `yaml:"mode"`
	JWKSFile      string `yaml:"jwks_file"`
	Issuer        string `yaml:"issuer"`
	Audience      string `yaml:"audience"`
	LeewaySeconds int    `yaml:"leeway_seconds"`
}

type InboxConfig struct {
	PruneDisabled        bool `yaml:"prune_disabled"`
	RetentionDays        int  `yaml:"retention_days"`
	PruneIntervalMinutes int  `yaml:"prune_interval_minutes"`
	PruneBatchSize       int  `yaml:"prune_batch_size"`
}

type AppConfig struct {
	Parallel  ParallelConfig  `yaml:"parallel"`
	Inbox     InboxConfig     `yaml:"inbox"`
	Auth      AuthConfig      `yaml:"auth"`
	Server    ServerConfig    `yaml:"server"`
	Database  DatabaseConfig  `yaml:"database"`
	Poller    PollerConfig    `yaml:"poller"`
	Kafka     KafkaConfig     `yaml:"kafka"`
	Outbox    OutboxConfig    `yaml:"outbox"`
	Endpoints EndpointsConfig `yaml:"endpoints"`
}

func (c *AppConfig) ConnString() string {
	return c.Database.DSN()
}

func (c *AppConfig) ReadTimeout() time.Duration {
	if c.Server.ReadTimeoutSeconds <= 0 {
		return 15 * time.Second
	}
	return time.Duration(c.Server.ReadTimeoutSeconds) * time.Second
}

func (c *AppConfig) WriteTimeout() time.Duration {
	if c.Server.WriteTimeoutSeconds <= 0 {
		return 15 * time.Second
	}
	return time.Duration(c.Server.WriteTimeoutSeconds) * time.Second
}

func (c *AppConfig) IdleTimeout() time.Duration {
	if c.Server.IdleTimeoutSeconds <= 0 {
		return 75 * time.Second
	}
	return time.Duration(c.Server.IdleTimeoutSeconds) * time.Second
}

const (
	DefaultShutdownDeadline = 35 * time.Second
	DefaultMaxBodyBytes     = int64(10 << 20)
	DefaultMaxOpenConns     = 50
	DefaultMaxIdleConns     = 10
)

func (c *AppConfig) MaxBodyLimit() int64 {
	if c.Server.MaxBodyBytes <= 0 {
		return DefaultMaxBodyBytes
	}
	return c.Server.MaxBodyBytes
}

func (c *AppConfig) ShutdownDeadline() time.Duration {
	if c.Server.ShutdownDeadlineSeconds <= 0 {
		return DefaultShutdownDeadline
	}
	return time.Duration(c.Server.ShutdownDeadlineSeconds) * time.Second
}

func (c *AppConfig) ConnMaxLifetime() time.Duration {
	if c.Database.ConnMaxLifetimeMinutes <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(c.Database.ConnMaxLifetimeMinutes) * time.Minute
}

func (c *AppConfig) LeaseDuration() time.Duration {
	if c.Poller.LeaseDurationSeconds <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.Poller.LeaseDurationSeconds) * time.Second
}

func (c *AppConfig) PollInterval() time.Duration {
	if c.Poller.PollIntervalMillis <= 0 {
		return 250 * time.Millisecond
	}
	return time.Duration(c.Poller.PollIntervalMillis) * time.Millisecond
}

func Load(path string) (*AppConfig, error) {
	cfg := &AppConfig{
		Auth: AuthConfig{LeewaySeconds: DefaultTokenLeewaySeconds},
		Server: ServerConfig{
			Name:                    "distributed-transaction-engine",
			BindAddress:             ":8080",
			AdminBindAddress:        ":8081",
			ReadTimeoutSeconds:      15,
			WriteTimeoutSeconds:     15,
			IdleTimeoutSeconds:      75,
			ShutdownDeadlineSeconds: int(DefaultShutdownDeadline / time.Second),
		},
		Database: DatabaseConfig{
			Port:                   "5432",
			User:                   "postgres",
			Name:                   "dist_tx_engine",
			SSLMode:                "disable",
			MaxOpenConns:           DefaultMaxOpenConns,
			MaxIdleConns:           DefaultMaxIdleConns,
			ConnMaxLifetimeMinutes: 5,
			MigrationsDir:          "database/migrations/sql",
		},
		Poller: PollerConfig{
			LeaseDurationSeconds: 30,
			PollIntervalMillis:   250,
		},
		Endpoints: EndpointsConfig{
			BasePath:      "/api/v1",
			HealthLive:    "/health/live",
			HealthReady:   "/health/ready",
			Sagas:         "/sagas",
			Workflows:     "/workflows",
			State:         "/state",
			CrashRecovery: "/crash-recovery",
		},
	}

	if path != "" {
		if content, err := os.ReadFile(path); err == nil {
			if yamlErr := yaml.Unmarshal(content, cfg); yamlErr != nil {
				return nil, fmt.Errorf("failed to parse config file: %w", yamlErr)
			}
		}
	}

	if envPort := os.Getenv("PORT"); envPort != "" {
		if !strings.HasPrefix(envPort, ":") {
			cfg.Server.BindAddress = ":" + envPort
		} else {
			cfg.Server.BindAddress = envPort
		}
	}
	if envAdminPort := os.Getenv("ADMIN_PORT"); envAdminPort != "" {
		if !strings.HasPrefix(envAdminPort, ":") {
			cfg.Server.AdminBindAddress = ":" + envAdminPort
		} else {
			cfg.Server.AdminBindAddress = envAdminPort
		}
	}

	if dbHost := os.Getenv("DB_HOST"); dbHost != "" {
		cfg.Database.Host = dbHost
	}
	if dbPort := os.Getenv("DB_PORT"); dbPort != "" {
		cfg.Database.Port = dbPort
	}
	if dbUser := os.Getenv("DB_USER"); dbUser != "" {
		cfg.Database.User = dbUser
	}
	if dbName := os.Getenv("DB_NAME"); dbName != "" {
		cfg.Database.Name = dbName
	}
	if dbSSLMode := os.Getenv("DB_SSLMODE"); dbSSLMode != "" {
		cfg.Database.SSLMode = dbSSLMode
	}

	if dbPassword := os.Getenv("DB_PASSWORD"); dbPassword != "" {
		cfg.Database.Password = dbPassword
	} else if secretFile := os.Getenv("DB_PASSWORD_FILE"); secretFile != "" {
		if secretContent, err := os.ReadFile(secretFile); err == nil {
			cfg.Database.Password = strings.TrimSpace(string(secretContent))
		}
	}

	if maxOpen := os.Getenv("DB_MAX_OPEN_CONNS"); maxOpen != "" {
		if val, err := strconv.Atoi(maxOpen); err == nil && val > 0 {
			cfg.Database.MaxOpenConns = val
		}
	}
	if maxIdle := os.Getenv("DB_MAX_IDLE_CONNS"); maxIdle != "" {
		if val, err := strconv.Atoi(maxIdle); err == nil && val > 0 {
			cfg.Database.MaxIdleConns = val
		}
	}
	if lifetime := strings.TrimSpace(os.Getenv("DB_CONN_MAX_LIFETIME")); lifetime != "" {
		if parsed, err := time.ParseDuration(lifetime); err == nil && parsed > 0 {
			cfg.Database.ConnMaxLifetimeMinutes = int(parsed / time.Minute)
			if cfg.Database.ConnMaxLifetimeMinutes < 1 {
				cfg.Database.ConnMaxLifetimeMinutes = 1
			}
		} else if minutes, convErr := strconv.Atoi(lifetime); convErr == nil && minutes > 0 {
			cfg.Database.ConnMaxLifetimeMinutes = minutes
		}
	}
	if deadline := os.Getenv("SHUTDOWN_DEADLINE_SECONDS"); deadline != "" {
		if val, err := strconv.Atoi(deadline); err == nil && val > 0 {
			cfg.Server.ShutdownDeadlineSeconds = val
		}
	}
	if limit := os.Getenv("MAX_REQUEST_BODY_BYTES"); limit != "" {
		if val, err := strconv.ParseInt(limit, 10, 64); err == nil && val > 0 {
			cfg.Server.MaxBodyBytes = val
		}
	}
	if expose := strings.ToLower(os.Getenv("EXPOSE_ERROR_DETAILS")); expose == "true" || expose == "false" {
		cfg.Server.ExposeErrorDetails = expose == "true"
	}
	if migDir := os.Getenv("MIGRATIONS_DIR"); migDir != "" {
		cfg.Database.MigrationsDir = migDir
	}

	if workerID := os.Getenv("WORKER_ID"); workerID != "" {
		cfg.Poller.WorkerID = workerID
	}
	if leaseDur := os.Getenv("POLLER_LEASE_DURATION_SECONDS"); leaseDur != "" {
		if val, err := strconv.Atoi(leaseDur); err == nil && val > 0 {
			cfg.Poller.LeaseDurationSeconds = val
		}
	}
	if enabled := strings.ToLower(strings.TrimSpace(os.Getenv("POLLER_ENABLED"))); enabled == "false" || enabled == "0" {
		cfg.Poller.Disabled = true
	} else if enabled == "true" || enabled == "1" {
		cfg.Poller.Disabled = false
	}
	if pollInt := os.Getenv("POLLER_INTERVAL_MILLIS"); pollInt != "" {
		if val, err := strconv.Atoi(pollInt); err == nil && val > 0 {
			cfg.Poller.PollIntervalMillis = val
		}
	}

	if kafkaBrokers := os.Getenv("KAFKA_BROKERS"); kafkaBrokers != "" {
		cfg.Kafka.Brokers = kafkaBrokers
	}

	cfg.Database.ApplyEnvironment()
	cfg.Kafka.ApplyEnvironment()
	cfg.Outbox.ApplyEnvironment()
	cfg.Inbox.ApplyEnvironment()
	cfg.Parallel.ApplyEnvironment()
	cfg.Auth.ApplyEnvironment()
	if err := cfg.Auth.Validate(os.Getenv("ENV")); err != nil {
		return nil, err
	}

	return cfg, nil
}

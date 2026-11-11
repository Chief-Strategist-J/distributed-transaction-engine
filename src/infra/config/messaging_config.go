package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTopicsManifest      = "messaging/topics/topics.yaml"
	defaultRelayIntervalMillis = 500
	defaultRelayBatchSize      = 100
)

func positiveInt(name string, current int) int {
	if value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name))); err == nil && value > 0 {
		return value
	}
	return current
}

func (k *KafkaConfig) ApplyEnvironment() {
	k.applySecurityEnvironment()
	k.WriteTimeoutMillis = positiveInt("KAFKA_WRITE_TIMEOUT_MS", k.WriteTimeoutMillis)
	k.BatchTimeoutMillis = positiveInt("KAFKA_BATCH_TIMEOUT_MS", k.BatchTimeoutMillis)
	k.ReplicationFactor = positiveInt("KAFKA_REPLICATION_FACTOR", k.ReplicationFactor)
	if manifest := strings.TrimSpace(os.Getenv("KAFKA_TOPICS_MANIFEST")); manifest != "" {
		k.TopicsManifest = manifest
	}
	if auto := strings.ToLower(strings.TrimSpace(os.Getenv("KAFKA_AUTO_CREATE_TOPICS"))); auto == "true" || auto == "false" {
		value := auto == "true"
		k.AutoCreateTopics = &value
	}
}

func (k *KafkaConfig) WriteTimeout() time.Duration {
	return time.Duration(k.WriteTimeoutMillis) * time.Millisecond
}

func (k *KafkaConfig) BatchTimeout() time.Duration {
	return time.Duration(k.BatchTimeoutMillis) * time.Millisecond
}

func (k *KafkaConfig) Manifest() string {
	if k.TopicsManifest == "" {
		return defaultTopicsManifest
	}
	return k.TopicsManifest
}

func (k *KafkaConfig) CreateTopicsOnStart(environment string) bool {
	if k.AutoCreateTopics != nil {
		return *k.AutoCreateTopics
	}
	return !secureEnvironments[strings.ToLower(strings.TrimSpace(environment))]
}

func (o *OutboxConfig) ApplyEnvironment() {
	if enabled := strings.ToLower(strings.TrimSpace(os.Getenv("OUTBOX_RELAY_ENABLED"))); enabled == "false" || enabled == "0" {
		o.RelayDisabled = true
	} else if enabled == "true" || enabled == "1" {
		o.RelayDisabled = false
	}
	o.RelayIntervalMillis = positiveInt("OUTBOX_RELAY_INTERVAL_MILLIS", o.RelayIntervalMillis)
	o.RelayBatchSize = positiveInt("OUTBOX_RELAY_BATCH_SIZE", o.RelayBatchSize)
}

func (o *OutboxConfig) RelayInterval() time.Duration {
	if o.RelayIntervalMillis <= 0 {
		return defaultRelayIntervalMillis * time.Millisecond
	}
	return time.Duration(o.RelayIntervalMillis) * time.Millisecond
}

func (o *OutboxConfig) BatchSize() int {
	if o.RelayBatchSize <= 0 {
		return defaultRelayBatchSize
	}
	return o.RelayBatchSize
}

func (k *KafkaConfig) Validate(environment string, brokers string) error {
	if !secureEnvironments[strings.ToLower(strings.TrimSpace(environment))] {
		return nil
	}
	if strings.TrimSpace(brokers) == "" {
		return errors.New("KAFKA_BROKERS is required in production and staging")
	}
	if k.AutoCreateTopics != nil && *k.AutoCreateTopics {
		return errors.New("KAFKA_AUTO_CREATE_TOPICS must not be enabled in production and staging; run the provision-topics command as a separate job")
	}
	return k.validateSecurity()
}

func (k *KafkaConfig) applySecurityEnvironment() {
	if tlsFlag := strings.ToLower(strings.TrimSpace(os.Getenv("KAFKA_TLS_ENABLED"))); tlsFlag == "true" || tlsFlag == "false" {
		k.TLSEnabled = tlsFlag == "true"
	}
	k.CAFile = firstSet(os.Getenv("KAFKA_CA_FILE"), k.CAFile)
	k.SASLMechanism = strings.ToUpper(firstSet(os.Getenv("KAFKA_SASL_MECHANISM"), k.SASLMechanism))
	k.SASLUsername = firstSet(os.Getenv("KAFKA_SASL_USERNAME"), k.SASLUsername)
	if password := os.Getenv("KAFKA_SASL_PASSWORD"); password != "" {
		k.SASLPassword = password
	} else if file := os.Getenv("KAFKA_SASL_PASSWORD_FILE"); file != "" {
		if content, err := os.ReadFile(file); err == nil {
			k.SASLPassword = strings.TrimSpace(string(content))
		}
	}
}

func (k *KafkaConfig) validateSecurity() error {
	if !k.TLSEnabled {
		return errors.New("KAFKA_TLS_ENABLED must be true in production and staging")
	}
	switch k.SASLMechanism {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		return errors.New("KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 in production and staging")
	}
	if k.SASLUsername == "" || k.SASLPassword == "" {
		return errors.New("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD (or KAFKA_SASL_PASSWORD_FILE) are required in production and staging")
	}
	return nil
}

const (
	defaultInboxRetentionDays = 30
	defaultPruneIntervalMin   = 60
	defaultPruneBatchSize     = 5000
)

func (i *InboxConfig) ApplyEnvironment() {
	if enabled := strings.ToLower(strings.TrimSpace(os.Getenv("INBOX_PRUNE_ENABLED"))); enabled == "false" || enabled == "0" {
		i.PruneDisabled = true
	} else if enabled == "true" || enabled == "1" {
		i.PruneDisabled = false
	}
	i.RetentionDays = positiveInt("INBOX_RETENTION_DAYS", i.RetentionDays)
	i.PruneIntervalMinutes = positiveInt("INBOX_PRUNE_INTERVAL_MINUTES", i.PruneIntervalMinutes)
	i.PruneBatchSize = positiveInt("INBOX_PRUNE_BATCH_SIZE", i.PruneBatchSize)
}

func (i *InboxConfig) Retention() time.Duration {
	if i.RetentionDays <= 0 {
		return defaultInboxRetentionDays * 24 * time.Hour
	}
	return time.Duration(i.RetentionDays) * 24 * time.Hour
}

func (i *InboxConfig) PruneInterval() time.Duration {
	if i.PruneIntervalMinutes <= 0 {
		return defaultPruneIntervalMin * time.Minute
	}
	return time.Duration(i.PruneIntervalMinutes) * time.Minute
}

func (i *InboxConfig) BatchSize() int {
	if i.PruneBatchSize <= 0 {
		return defaultPruneBatchSize
	}
	return i.PruneBatchSize
}

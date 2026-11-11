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
	return nil
}

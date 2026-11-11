package chassis

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"distributed-transaction-engine/src/infra/config"
	"distributed-transaction-engine/src/infra/messaging"
)

const topicsReconcileTimeout = 30 * time.Second

func KafkaSecurity(cfg *config.AppConfig) messaging.Security {
	return messaging.Security{
		TLSEnabled: cfg.Kafka.TLSEnabled, CAFile: cfg.Kafka.CAFile, Mechanism: cfg.Kafka.SASLMechanism,
		Username: cfg.Kafka.SASLUsername, Password: cfg.Kafka.SASLPassword,
	}
}

func kafkaBrokers(cfg *config.AppConfig) []string {
	return ParseBrokers(firstNonEmpty(cfg.Kafka.Brokers, os.Getenv("KAFKA_BROKERS")))
}

func reconcileTopics(cfg *config.AppConfig, create bool) (messaging.Report, error) {
	brokers := kafkaBrokers(cfg)
	if len(brokers) == 0 {
		return messaging.Report{}, fmt.Errorf("no kafka brokers configured")
	}
	manifest, _, err := messaging.LoadManifest(cfg.Kafka.Manifest())
	if err != nil {
		return messaging.Report{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), topicsReconcileTimeout)
	defer cancel()
	return messaging.ReconcileSecure(ctx, brokers, manifest.WithReplicationFactor(cfg.Kafka.ReplicationFactor), create, KafkaSecurity(cfg))
}

func runProvisionTopics(cfg *config.AppConfig) int {
	report, err := reconcileTopics(cfg, true)
	if err != nil {
		log.Printf("Topic provisioning failed: %v\n", err)
		return ExitDependency
	}
	log.Printf("Topics provisioned: created=%v existing=%v mismatched=%v\n", report.Created, report.Existing, report.Mismatched)
	if len(report.Mismatched) > 0 {
		return ExitDependency
	}
	return ExitSuccess
}

func ensureTopicsOnStart(cfg *config.AppConfig, environment string) int {
	if len(kafkaBrokers(cfg)) == 0 {
		return ExitSuccess
	}
	secure := config.IsSecureEnvironment(environment)
	report, err := reconcileTopics(cfg, cfg.Kafka.CreateTopicsOnStart(environment))
	if err != nil {
		log.Printf("WARNING: kafka topics could not be verified: %v\n", err)
		return ExitSuccess
	}
	if !report.Healthy() {
		log.Printf("Kafka topics do not match the manifest: missing=%v mismatched=%v\n", report.Missing, report.Mismatched)
		if secure {
			return ExitDependency
		}
	}
	if len(report.Created) > 0 {
		log.Printf("Created kafka topics: %s\n", strings.Join(report.Created, ","))
	}
	return ExitSuccess
}

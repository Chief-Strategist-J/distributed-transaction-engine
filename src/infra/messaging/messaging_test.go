package messaging

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func writeManifest(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "topics.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadManifest_RejectsInvalidManifests(t *testing.T) {
	cases := map[string]string{
		"empty":              "topics: []\n",
		"not_yaml":           "topics: [unclosed\n",
		"blank_name":         "topics:\n  - {name: '', partitions: 1, replication_factor: 1}\n",
		"illegal_characters": "topics:\n  - {name: 'bad topic', partitions: 1, replication_factor: 1}\n",
		"dot_name":           "topics:\n  - {name: '..', partitions: 1, replication_factor: 1}\n",
		"too_long":           "topics:\n  - {name: '" + strings.Repeat("a", 250) + "', partitions: 1, replication_factor: 1}\n",
		"zero_partitions":    "topics:\n  - {name: a, partitions: 0, replication_factor: 1}\n",
		"zero_replication":   "topics:\n  - {name: a, partitions: 1, replication_factor: 0}\n",
		"negative_retention": "topics:\n  - {name: a, partitions: 1, replication_factor: 1, retention_ms: -1}\n",
		"duplicate":          "topics:\n  - {name: a, partitions: 1, replication_factor: 1}\n  - {name: a, partitions: 2, replication_factor: 1}\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := LoadManifest(writeManifest(t, content)); err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		})
	}
	if _, _, err := LoadManifest(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("a missing manifest must be an error")
	}
}

func TestManifest_ReplicationFactorOverrideDoesNotMutateTheOriginal(t *testing.T) {
	original := Manifest{Topics: []TopicSpec{{Name: "a", Partitions: 1, ReplicationFactor: 1}}}
	overridden := original.WithReplicationFactor(3)
	if overridden.Topics[0].ReplicationFactor != 3 || original.Topics[0].ReplicationFactor != 1 {
		t.Fatalf("override must copy: %+v %+v", original, overridden)
	}
	if original.WithReplicationFactor(0).Topics[0].ReplicationFactor != 1 {
		t.Fatal("a non-positive override must be ignored")
	}
}

func TestRepositoryManifestIsValidAndTheLockMatchesIt(t *testing.T) {
	manifest, digest, err := LoadManifest("../../../messaging/topics/topics.yaml")
	if err != nil {
		t.Fatalf("committed manifest is invalid: %v", err)
	}
	raw, err := os.ReadFile("../../../messaging/topics.lock")
	if err != nil {
		t.Fatal(err)
	}
	var lock struct {
		ManifestSHA256 string   `json:"manifest_sha256"`
		Topics         []string `json:"topics"`
	}
	if err = json.Unmarshal(raw, &lock); err != nil {
		t.Fatal(err)
	}
	declared := make([]string, 0, len(manifest.Topics))
	for _, topic := range manifest.Topics {
		declared = append(declared, topic.Name)
	}
	sort.Strings(declared)
	if lock.ManifestSHA256 != digest || strings.Join(lock.Topics, ",") != strings.Join(declared, ",") {
		t.Fatalf("messaging/topics.lock is stale: regenerate it from topics.yaml (sha256 %s)", digest)
	}
}

func kafkaBroker(t *testing.T) []string {
	t.Helper()
	address := os.Getenv("KAFKA_BROKERS")
	if address == "" {
		address = "127.0.0.1:9092"
	}
	conn, err := kafka.DialContext(context.Background(), "tcp", address)
	if err != nil {
		t.Skipf("kafka is not reachable: %v", err)
	}
	_ = conn.Close()
	return []string{address}
}

func uniqueManifest(partitions int) (Manifest, string) {
	name := "reconcile-" + uuid.NewString()
	return Manifest{Topics: []TopicSpec{{Name: name, Partitions: partitions, ReplicationFactor: 1, RetentionMillis: 3600000}}}, name
}

func TestReconcile_ValidateOnlyReportsMissingWithoutCreating(t *testing.T) {
	brokers := kafkaBroker(t)
	manifest, name := uniqueManifest(2)
	report, err := Reconcile(context.Background(), brokers, manifest, false)
	if err != nil || report.Healthy() || len(report.Missing) != 1 || report.Missing[0] != name {
		t.Fatalf("expected one missing topic, got %+v err=%v", report, err)
	}
	again, _ := Reconcile(context.Background(), brokers, manifest, false)
	if len(again.Missing) != 1 {
		t.Fatal("validate-only mode must not create topics")
	}
}

func TestReconcile_CreatesMissingTopicsIdempotently(t *testing.T) {
	brokers := kafkaBroker(t)
	manifest, name := uniqueManifest(3)
	report, err := Reconcile(context.Background(), brokers, manifest, true)
	if err != nil || len(report.Created) != 1 || report.Created[0] != name || !report.Healthy() {
		t.Fatalf("expected the topic to be created, got %+v err=%v", report, err)
	}
	second, err := Reconcile(context.Background(), brokers, manifest, true)
	if err != nil || len(second.Created) != 0 || len(second.Existing) != 1 || !second.Healthy() {
		t.Fatalf("second run must be a no-op, got %+v err=%v", second, err)
	}
}

func TestReconcile_ReportsPartitionCountDrift(t *testing.T) {
	brokers := kafkaBroker(t)
	manifest, name := uniqueManifest(3)
	if _, err := Reconcile(context.Background(), brokers, manifest, true); err != nil {
		t.Fatal(err)
	}
	drifted := Manifest{Topics: []TopicSpec{{Name: name, Partitions: 5, ReplicationFactor: 1}}}
	report, err := Reconcile(context.Background(), brokers, drifted, true)
	if err != nil || report.Healthy() || len(report.Mismatched) != 1 || !strings.Contains(report.Mismatched[0], name) {
		t.Fatalf("partition drift must be reported, got %+v err=%v", report, err)
	}
}

func TestReconcile_UnreachableBrokerIsAnError(t *testing.T) {
	manifest, _ := uniqueManifest(1)
	if _, err := Reconcile(context.Background(), []string{"127.0.0.1:1"}, manifest, true); err == nil {
		t.Fatal("an unreachable cluster must be an error, not an empty report")
	}
}

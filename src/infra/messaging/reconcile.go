package messaging

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	dialTimeout      = 5 * time.Second
	visibilityWindow = 15 * time.Second
	visibilityPoll   = 100 * time.Millisecond
)

type Report struct {
	Existing   []string
	Created    []string
	Missing    []string
	Mismatched []string
}

func (r Report) Healthy() bool {
	return len(r.Missing) == 0 && len(r.Mismatched) == 0
}

func Reconcile(ctx context.Context, brokers []string, manifest Manifest, create bool) (Report, error) {
	return ReconcileSecure(ctx, brokers, manifest, create, Security{})
}

func ReconcileSecure(ctx context.Context, brokers []string, manifest Manifest, create bool, security Security) (Report, error) {
	dialer, err := security.Dialer()
	if err != nil {
		return Report{}, err
	}
	controller, err := dialController(ctx, brokers, dialer)
	if err != nil {
		return Report{}, err
	}
	defer controller.Close()
	partitions, err := existingPartitions(controller)
	if err != nil {
		return Report{}, err
	}
	report, missing := classify(manifest, partitions)
	if create && len(missing) > 0 {
		if err = createTopics(controller, missing); err != nil {
			return report, err
		}
		if err = awaitTopics(ctx, controller, missing); err != nil {
			return report, err
		}
		report.Created, report.Missing = names(missing), nil
	}
	return report, nil
}

func dialController(ctx context.Context, brokers []string, dialer *kafka.Dialer) (*kafka.Conn, error) {
	var lastErr error
	for _, broker := range brokers {
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}
		controller, err := conn.Controller()
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	}
	return nil, fmt.Errorf("no reachable kafka controller among %s: %w", strings.Join(brokers, ","), lastErr)
}

func existingPartitions(conn *kafka.Conn) (map[string]int, error) {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read kafka topic metadata: %w", err)
	}
	counts := make(map[string]int)
	for _, partition := range partitions {
		counts[partition.Topic]++
	}
	return counts, nil
}

func readyPartitions(conn *kafka.Conn) (map[string]int, error) {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read kafka topic metadata: %w", err)
	}
	counts := make(map[string]int)
	for _, partition := range partitions {
		if partition.Leader.ID >= 0 && partition.Leader.Host != "" {
			counts[partition.Topic]++
		}
	}
	return counts, nil
}

func classify(manifest Manifest, partitions map[string]int) (Report, []TopicSpec) {
	var report Report
	var missing []TopicSpec
	for _, topic := range manifest.Topics {
		count, found := partitions[topic.Name]
		switch {
		case !found:
			missing = append(missing, topic)
			report.Missing = append(report.Missing, topic.Name)
		case count != topic.Partitions:
			report.Mismatched = append(report.Mismatched, fmt.Sprintf("%s has %d partitions, manifest declares %d", topic.Name, count, topic.Partitions))
		default:
			report.Existing = append(report.Existing, topic.Name)
		}
	}
	sort.Strings(report.Existing)
	return report, missing
}

func createTopics(conn *kafka.Conn, missing []TopicSpec) error {
	configs := make([]kafka.TopicConfig, 0, len(missing))
	for _, topic := range missing {
		config := kafka.TopicConfig{Topic: topic.Name, NumPartitions: topic.Partitions, ReplicationFactor: topic.ReplicationFactor}
		if topic.RetentionMillis > 0 {
			config.ConfigEntries = []kafka.ConfigEntry{{ConfigName: "retention.ms", ConfigValue: strconv.FormatInt(topic.RetentionMillis, 10)}}
		}
		configs = append(configs, config)
	}
	if err := conn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("create kafka topics: %w", err)
	}
	return nil
}

func names(topics []TopicSpec) []string {
	out := make([]string, 0, len(topics))
	for _, topic := range topics {
		out = append(out, topic.Name)
	}
	return out
}

func awaitTopics(ctx context.Context, conn *kafka.Conn, expected []TopicSpec) error {
	deadline := time.Now().Add(visibilityWindow)
	for {
		partitions, err := readyPartitions(conn)
		if err == nil && allVisible(partitions, expected) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("created topics did not become visible within %s", visibilityWindow)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(visibilityPoll):
		}
	}
}

func allVisible(partitions map[string]int, expected []TopicSpec) bool {
	for _, topic := range expected {
		if partitions[topic.Name] != topic.Partitions {
			return false
		}
	}
	return true
}

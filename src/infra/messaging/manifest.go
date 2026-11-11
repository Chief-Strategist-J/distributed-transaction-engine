package messaging

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

var topicNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

type TopicSpec struct {
	Name              string `yaml:"name"`
	Partitions        int    `yaml:"partitions"`
	ReplicationFactor int    `yaml:"replication_factor"`
	RetentionMillis   int64  `yaml:"retention_ms"`
}

type Manifest struct {
	Topics []TopicSpec `yaml:"topics"`
}

func LoadManifest(path string) (Manifest, string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, "", fmt.Errorf("read topics manifest: %w", err)
	}
	var manifest Manifest
	if err = yaml.Unmarshal(content, &manifest); err != nil {
		return Manifest{}, "", fmt.Errorf("parse topics manifest: %w", err)
	}
	if err = manifest.Validate(); err != nil {
		return Manifest{}, "", err
	}
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:]), nil
}

func (m Manifest) Validate() error {
	if len(m.Topics) == 0 {
		return errors.New("topics manifest declares no topics")
	}
	seen := make(map[string]bool, len(m.Topics))
	for _, topic := range m.Topics {
		if err := topic.validate(); err != nil {
			return err
		}
		if seen[topic.Name] {
			return fmt.Errorf("topic %q is declared twice", topic.Name)
		}
		seen[topic.Name] = true
	}
	return nil
}

func (t TopicSpec) validate() error {
	if !topicNamePattern.MatchString(t.Name) || t.Name == "." || t.Name == ".." {
		return fmt.Errorf("invalid topic name %q", t.Name)
	}
	if t.Partitions < 1 || t.ReplicationFactor < 1 || t.RetentionMillis < 0 {
		return fmt.Errorf("topic %q needs partitions >= 1, replication_factor >= 1 and retention_ms >= 0", t.Name)
	}
	return nil
}

func (m Manifest) WithReplicationFactor(factor int) Manifest {
	if factor < 1 {
		return m
	}
	topics := make([]TopicSpec, len(m.Topics))
	copy(topics, m.Topics)
	for index := range topics {
		topics[index].ReplicationFactor = factor
	}
	return Manifest{Topics: topics}
}

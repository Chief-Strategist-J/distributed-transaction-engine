package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"distributed-transaction-engine/src/shared/errors"
)

type KafkaPublisherAdapter struct {
	brokerAddress string
	writer        *kafka.Writer
}

func NewKafkaPublisherAdapter(brokerAddress string) *KafkaPublisherAdapter {
	if brokerAddress == "" {
		brokerAddress = "127.0.0.1:9092"
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}
	return &KafkaPublisherAdapter{
		brokerAddress: brokerAddress,
		writer:        writer,
	}
}

func (a *KafkaPublisherAdapter) PublishMessage(
	ctx context.Context,
	topic string,
	partitionKey string,
	headers map[string]interface{},
	payload []byte,
) error {
	var kafkaHeaders []kafka.Header
	for k, v := range headers {
		var valBytes []byte
		switch val := v.(type) {
		case string:
			valBytes = []byte(val)
		case []byte:
			valBytes = val
		default:
			valBytes, _ = json.Marshal(val)
		}
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: valBytes,
		})
	}

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(partitionKey),
		Value:   payload,
		Headers: kafkaHeaders,
		Time:    time.Now().UTC(),
	}

	if err := a.writer.WriteMessages(ctx, msg); err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to produce to kafka broker %s topic %s: %v", a.brokerAddress, topic, err))
	}
	return nil
}

func (a *KafkaPublisherAdapter) Close() error {
	return a.writer.Close()
}

type MemoryKafkaPublisherAdapter struct {
	Messages []kafka.Message
}

func NewMemoryKafkaPublisherAdapter() *MemoryKafkaPublisherAdapter {
	return &MemoryKafkaPublisherAdapter{
		Messages: make([]kafka.Message, 0),
	}
}

func (a *MemoryKafkaPublisherAdapter) PublishMessage(
	_ context.Context,
	topic string,
	partitionKey string,
	headers map[string]interface{},
	payload []byte,
) error {
	var kafkaHeaders []kafka.Header
	for k, v := range headers {
		valBytes, _ := json.Marshal(v)
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: valBytes})
	}
	a.Messages = append(a.Messages, kafka.Message{
		Topic:   topic,
		Key:     []byte(partitionKey),
		Value:   payload,
		Headers: kafkaHeaders,
	})
	return nil
}

func EnsureKafkaTopic(brokerHost string, brokerPort int, topic string) error {
	conn, err := kafka.Dial("tcp", net.JoinHostPort(brokerHost, strconv.Itoa(brokerPort)))
	if err != nil {
		return err
	}
	defer conn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}
	return conn.CreateTopics(topicConfigs...)
}

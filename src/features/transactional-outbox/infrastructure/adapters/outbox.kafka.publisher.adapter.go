package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
	"time"
)

type KafkaPublisherOptions struct {
	WriteTimeout time.Duration
	BatchTimeout time.Duration
	BatchSize    int
	Transport    *kafka.Transport
}

const (
	defaultWriteTimeout = 5 * time.Second
	defaultBatchTimeout = 10 * time.Millisecond
	defaultBatchSize    = 100
)

type KafkaPublisherAdapter struct {
	brokerAddress string
	writer        *kafka.Writer
}

func NewKafkaPublisherAdapter(brokerAddress string) *KafkaPublisherAdapter {
	return NewKafkaPublisherAdapterWithOptions(brokerAddress, KafkaPublisherOptions{})
}

func NewKafkaPublisherAdapterWithOptions(brokerAddress string, options KafkaPublisherOptions) *KafkaPublisherAdapter {
	if brokerAddress == "" {
		brokerAddress = "127.0.0.1:9092"
	}
	if options.WriteTimeout <= 0 {
		options.WriteTimeout = defaultWriteTimeout
	}
	if options.BatchTimeout <= 0 {
		options.BatchTimeout = defaultBatchTimeout
	}
	if options.BatchSize <= 0 {
		options.BatchSize = defaultBatchSize
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchSize:    options.BatchSize,
		BatchTimeout: options.BatchTimeout,
		WriteTimeout: options.WriteTimeout,
		ReadTimeout:  options.WriteTimeout,
	}
	if options.Transport != nil {
		writer.Transport = options.Transport
	}
	return &KafkaPublisherAdapter{
		brokerAddress: brokerAddress,
		writer:        writer,
	}
}

func buildKafkaMessage(request contracts.PublishRequest) kafka.Message {
	kafkaHeaders := make([]kafka.Header, 0, len(request.Headers))
	for key, value := range request.Headers {
		var valueBytes []byte
		switch typed := value.(type) {
		case string:
			valueBytes = []byte(typed)
		case []byte:
			valueBytes = typed
		default:
			valueBytes, _ = json.Marshal(typed)
		}
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: key, Value: valueBytes})
	}
	return kafka.Message{
		Topic:   request.Topic,
		Key:     []byte(request.PartitionKey),
		Value:   request.Payload,
		Headers: kafkaHeaders,
		Time:    time.Now().UTC(),
	}
}

func (a *KafkaPublisherAdapter) PublishBatch(ctx context.Context, requests []contracts.PublishRequest) (spanResult0 []error) {
	var spanErr error
	defer observability.TraceEnter(&spanErr)()
	outcomes := make([]error, len(requests))
	if len(requests) == 0 {
		return outcomes
	}
	messages := make([]kafka.Message, len(requests))
	for index, request := range requests {
		messages[index] = buildKafkaMessage(request)
	}
	err := a.writer.WriteMessages(ctx, messages...)
	if err == nil {
		return outcomes
	}
	var perMessage kafka.WriteErrors
	for index := range outcomes {
		failure := err
		if stderrors.As(err, &perMessage) && index < len(perMessage) {
			failure = perMessage[index]
		}
		if failure != nil {
			outcomes[index] = errors.NewInternalError(fmt.Sprintf("failed to produce to kafka broker %s topic %s: %v", a.brokerAddress, requests[index].Topic, failure))
		}
	}
	spanErr = err
	return outcomes
}

func (a *KafkaPublisherAdapter) PublishMessage(
	ctx context.Context,
	topic string,
	partitionKey string,
	headers map[string]interface{},
	payload []byte,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
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
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
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

func EnsureKafkaTopic(brokerHost string, brokerPort int, topic string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
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

func (a *MemoryKafkaPublisherAdapter) PublishBatch(ctx context.Context, requests []contracts.PublishRequest) (spanResult0 []error) {
	defer observability.TraceEnter(nil)()
	outcomes := make([]error, len(requests))
	for index, request := range requests {
		outcomes[index] = a.PublishMessage(ctx, request.Topic, request.PartitionKey, request.Headers, request.Payload)
	}
	return outcomes
}

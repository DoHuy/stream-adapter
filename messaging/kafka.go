package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pborman/uuid"
)

const defaultMaxProcessingTime = time.Second

type kafkaClient struct {
	producer          sarama.SyncProducer
	partitionProducer sarama.SyncProducer
	debug             bool
}

type KafkaClientConfig struct {
	Brokers            []string `json:"brokers,omitempty"`
	ClientIDPrefix     string   `json:"clientIdPrefix,omitempty"`
	ProducerTimeout    string   `json:"producerTimeout,omitempty"`
	ProducerMaxRetries int      `json:"producerMaxRetries,omitempty"`
	Debug              bool     `json:"debug"`
}

func NewKafkaClient(config KafkaClientConfig) (Client, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("must specify at least one kafka broker")
	}

	if config.Debug {
		sarama.Logger = log.New(os.Stderr, "[sarama] ", log.LstdFlags)
	}

	// setup producer - produces across partitions
	cfg := sarama.NewConfig()
	cfg.ClientID = config.ClientIDPrefix + uuid.New()
	cfg.Producer.Partitioner = sarama.NewHashPartitioner
	// cfg.Producer.Compression = sarama.CompressionSnappy // enable for message compression
	cfg.Producer.Return.Successes = true // enabled for sync producer
	cfg.Producer.Return.Errors = true    // enabled for sync producer
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 1 * time.Second
	cfg.Metadata.Retry.Max = 5
	cfg.Metadata.Retry.Backoff = 1 * time.Second

	if config.ProducerMaxRetries > 0 {
		cfg.Producer.Retry.Max = config.ProducerMaxRetries
	}

	if config.ProducerTimeout != "" {
		var err error
		cfg.Producer.Timeout, err = time.ParseDuration(config.ProducerTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid producer timeout value %q: %s", config.ProducerTimeout, err)
		}
	}

	producer, err := sarama.NewSyncProducer(config.Brokers, cfg)
	if err != nil {
		return nil, err
	}

	// this producer produces to a single manually-specified partition
	cfgCopy := *cfg
	cfgCopy.Producer.Partitioner = sarama.NewManualPartitioner
	parititonProducer, err := sarama.NewSyncProducer(config.Brokers, &cfgCopy)
	if err != nil {
		return nil, err
	}

	return &kafkaClient{
		producer:          producer,
		partitionProducer: parititonProducer,
		debug:             config.Debug,
	}, nil
}

func (k *kafkaClient) Produce(ctx context.Context, topic string, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	return k.produce(ctx, k.producer, msg)
}

func (k *kafkaClient) ProduceWithPartition(ctx context.Context, topic string, partition int32, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(value),
	}
	return k.produce(ctx, k.partitionProducer, msg)
}

func (k *kafkaClient) Close() error {
	return k.producer.Close()
}

func (k *kafkaClient) logf(format string, v ...interface{}) {
	if k.debug {
		log.Printf(format, v...)
	}
}

func (k *kafkaClient) produce(ctx context.Context, producer sarama.SyncProducer, msg *sarama.ProducerMessage) error {
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	k.logf("Message stored in topic(%s)/partition(%d)/offset(%d)/bytes(%d)", msg.Topic, partition, offset, msg.Value.Length())
	return nil
}

package messaging

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	messages "github.com/veritone/edge-messages"
)

const (
	defaultEngineStatusTopic     = "engine_status"
	defaultIngestionRequestTopic = "ingestion_queue_0" // default to low priority topic
)

type Client interface {
	Produce(ctx context.Context, topic string, key string, value []byte) error
	ProduceWithPartition(ctx context.Context, topic string, partition int32, key string, value []byte) error
	Close() error
}

type ClientConfig struct {
	MessageTopics MessageTopicConfig `json:"topics,omitempty"`
	Kafka         KafkaClientConfig  `json:"kafka,omitempty"`
}

type MessageTopicConfig struct {
	EngineStatus     string `json:"engineHeartbeat,omitempty"`
	IngestionRequest string `json:"ingestionRequest,omitempty"`
}

func (m *MessageTopicConfig) defaults() {
	if m.EngineStatus == "" {
		m.EngineStatus = defaultEngineStatusTopic
	}
	if m.IngestionRequest == "" {
		m.IngestionRequest = defaultIngestionRequestTopic
	}
}

type MessageContext struct {
	TDOID      string
	JobID      string
	TaskID     string
	EngineID   string
	InstanceID string
}

type Helper struct {
	Topics     MessageTopicConfig
	client     Client
	messageCtx MessageContext
}

func NewHelper(client Client, config ClientConfig, messageCtx MessageContext) *Helper {
	config.MessageTopics.defaults()

	return &Helper{
		Topics:     config.MessageTopics,
		client:     client,
		messageCtx: messageCtx,
	}
}

func (h *Helper) ProduceHeartbeatMessage(ctx context.Context, msg messages.EngineHeartbeat) error {
	msg.JobID = h.messageCtx.JobID
	msg.TaskID = h.messageCtx.TaskID
	msg.TDOID = h.messageCtx.TDOID
	msg.EngineID = h.messageCtx.EngineID
	msg.EngineInstanceID = h.messageCtx.InstanceID

	return h.send(ctx, h.Topics.EngineStatus, -1, msg.Key(), msg)
}

func (h *Helper) ProduceOffineIngestionRequestMessage(ctx context.Context, msg messages.OfflineIngestionRequest) error {
	msg.SourceTaskSummary.JobID = h.messageCtx.JobID
	msg.SourceTaskSummary.TaskID = h.messageCtx.TaskID
	msg.SourceTaskSummary.EngineID = h.messageCtx.EngineID
	msg.SourceTaskSummary.EngineInstanceID = h.messageCtx.InstanceID

	return h.send(ctx, h.Topics.IngestionRequest, -1, msg.Key(), msg)
}

func (h *Helper) ProduceStreamInitMessage(ctx context.Context, topic string, partition int32, msg messages.StreamInit) error {
	msg.JobID = h.messageCtx.JobID
	msg.TaskID = h.messageCtx.TaskID
	msg.TDOID = h.messageCtx.TDOID

	return h.send(ctx, topic, partition, msg.Key(), msg)
}

func (h *Helper) ProduceStreamEOFMessage(ctx context.Context, topic string, partition int32, msg messages.StreamEOF) error {
	return h.send(ctx, topic, partition, msg.Key(), msg)
}

func (h *Helper) ProduceRawStreamMessage(ctx context.Context, topic string, partition int32, msg *messages.RawStream) error {
	return h.send(ctx, topic, partition, msg.Key(), msg.Value)
}

func (h *Helper) send(ctx context.Context, topic string, partition int32, key string, value interface{}) error {
	if h.client == nil {
		panic("message producer's client instance is nil")
	}

	valBytes, ok := value.([]byte)
	if !ok {
		var err error
		// if value is not a byte slice, JSON encode it
		valBytes, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}

	// for invalid partition just call regular produce
	if partition < 0 {
		return h.client.Produce(ctx, topic, key, valBytes)
	}

	return h.client.ProduceWithPartition(ctx, topic, partition, key, valBytes)
}

func ParseStreamTopic(topicStr string) (string, int32, string) {
	result := strings.Split(topicStr, ":")
	if len(result) < 2 {
		return "", -1, ""
	}

	partition, err := strconv.Atoi(result[1])
	if err != nil {
		return "", -1, ""
	}

	var prefix string
	if len(result) > 2 {
		prefix = result[2]
	}

	return result[0], int32(partition), prefix
}

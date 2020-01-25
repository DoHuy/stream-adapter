package messaging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"strconv"
	"time"
)

type kafkaTestSuite struct {
	suite.Suite
	hosts                       []string
	topic                       string
	capturedMessagesByPartition map[int32][]*sarama.ConsumerMessage
	capturedMessagesLast        map[string]*sarama.ConsumerMessage // Last message captured with a given key

	groupId             string
	groupConsumerClient *cluster.Client
	groupConsumer       *cluster.Consumer
}

func (t *kafkaTestSuite) SetupSuite() {
	t.hosts = []string{"kafka1:9092", "kafka2:9092"}
	t.topic = "webstream_adapter_test_topic"
	var err error
	err = t.createTopic(t.hosts, t.topic, 8)
	if err != nil {
		assert.Contains(t.T(), err.Error(), "Topic with this name already exists")
	}

	t.capturedMessagesByPartition = make(map[int32][]*sarama.ConsumerMessage)
	t.capturedMessagesLast = make(map[string]*sarama.ConsumerMessage)
	_, clusterConfig := GetDefaultConfig()
	t.groupConsumerClient, err = cluster.NewClient(t.hosts, clusterConfig)
	assert.Nil(t.T(), err)
	t.groupId = "cg_" + t.topic + strconv.Itoa(time.Now().Second())
	t.groupConsumer, err = cluster.NewConsumerFromClient(t.groupConsumerClient, t.groupId, []string{t.topic})
	assert.Nil(t.T(), err)
	t.runKafkaMessageCapture()
	time.Sleep(time.Second * 5)
}

func (t *kafkaTestSuite) SetupTest() {
	t.capturedMessagesByPartition = make(map[int32][]*sarama.ConsumerMessage)
	t.capturedMessagesLast = make(map[string]*sarama.ConsumerMessage)
}

func (t *kafkaTestSuite) TearDownSuite() {
	t.groupConsumer.Close()
	t.groupConsumerClient.Close()
}

func (t *kafkaTestSuite) TestNewKafkaClient() {
	config := KafkaClientConfig{
		Brokers:            []string{"kafka1:9092", "kafka2:9092"},
		ClientIDPrefix:     "clientIdPrefix",
		ProducerTimeout:    "2s",
		ProducerMaxRetries: 3,
		Debug:              true,
	}
	client, err := NewKafkaClient(config)
	assert.Nil(t.T(), err)
	assert.IsType(t.T(), new(kafkaClient), client)
}

type keyValue struct {
	key   string
	value []byte
}

func (t *kafkaTestSuite) TestProduce() {
	config := KafkaClientConfig{
		Brokers:            []string{"kafka1:9092", "kafka2:9092"},
		ClientIDPrefix:     "clientIdPrefix",
		ProducerTimeout:    "2s",
		ProducerMaxRetries: 3,
		Debug:              true,
	}
	client, err := NewKafkaClient(config)
	defer client.Close()
	assert.Nil(t.T(), err)
	var keyValues []keyValue
	for i := 0; i < 10; i++ {
		keyValues = append(keyValues, keyValue{"key_" + strconv.Itoa(i), []byte("value_" + strconv.Itoa(i))})
	}
	for _, pair := range keyValues {
		err := client.Produce(context.Background(), t.topic, pair.key, pair.value)
		assert.Nil(t.T(), err)
		printf("Produced message: %s", pair.key)
	}
	time.Sleep(time.Millisecond * 500)
	assert.Equal(t.T(), len(keyValues), len(t.capturedMessagesLast))
	for _, pair := range keyValues {
		_, ok := t.capturedMessagesLast[pair.key]
		assert.True(t.T(), ok, fmt.Sprintf("kafka message key %s not found", pair.key))
	}
}

func (t *kafkaTestSuite) TestProduce_MessagesWithSameKeyFoundInSamePartition() {
	config := KafkaClientConfig{
		Brokers:            []string{"kafka1:9092", "kafka2:9092"},
		ClientIDPrefix:     "clientIdPrefix",
		ProducerTimeout:    "2s",
		ProducerMaxRetries: 3,
		Debug:              true,
	}
	client, err := NewKafkaClient(config)
	defer client.Close()
	assert.Nil(t.T(), err)
	var keyValues []keyValue
	// 20 messages with 4 different keys (key_0, key_1, key_2, key_3)
	const numMessages = 20
	const numKeys = 4
	const numMessagesPerKey = numMessages / numKeys
	for i := 0; i < numMessages; i++ {
		keyValues = append(keyValues, keyValue{"key_" + strconv.Itoa(i%4), []byte("value_" + strconv.Itoa(i))})
	}
	for _, pair := range keyValues {
		err := client.Produce(context.Background(), t.topic, pair.key, pair.value)
		assert.Nil(t.T(), err)
		printf("Produced message: %s", pair.key)
	}
	time.Sleep(time.Millisecond * 500)

	for key, message := range t.capturedMessagesLast {
		partitionMessages := t.capturedMessagesByPartition[message.Partition]
		count := 0
		for _, partitionMessage := range partitionMessages {
			if string(partitionMessage.Key) == key {
				count++
			}
		}
		assert.Equal(t.T(), numMessagesPerKey, count, fmt.Sprintf("There should be 4 messages with key %s in partition %d", key, message.Partition))
	}
}

func (k *kafkaTestSuite) runKafkaMessageCapture() {
	go func() {
		defer k.groupConsumer.Close()
		for {
			select {
			case msg := <-k.groupConsumer.Messages():
				if msg != nil {
					printf("Received message: %s", msg.Key)
					k.capturedMessagesByPartition[msg.Partition] = append(k.capturedMessagesByPartition[msg.Partition], msg)
					k.capturedMessagesLast[string(msg.Key)] = msg
				}
			case err := <-k.groupConsumer.Errors():
				if err != nil {
					printf("Kafka error: %s", err.Error())
				}
				return
			case note := <-k.groupConsumer.Notifications():
				if note != nil {
					printf("%v: Kafka notification: %+v", time.Now().Format(time.StampMilli), note)
				}
			}
		}
	}()
}

func (m *kafkaTestSuite) createTopic(hosts []string, topic string, numPartitions int32) error {
	var err error
	c, _ := GetDefaultConfig()
	client, err := sarama.NewClient(hosts, c)
	if err != nil {
		return err
	}
	controllerBroker, err := client.Controller()
	if err != nil {
		return err
	}
	t := &sarama.CreateTopicsRequest{}
	t.Timeout = time.Second * 5
	t.Version = 2
	t.TopicDetails = make(map[string]*sarama.TopicDetail)
	t.TopicDetails[topic] = &sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	}
	res, err := controllerBroker.CreateTopics(t)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, err := range res.TopicErrors {
		if err.Err != sarama.ErrNoError {
			buf.WriteString(err.Err.Error() + ",")
		}
	}
	if buf.Len() > 0 {
		err = errors.New(buf.String())
		return err
	}
	return nil
}

func GetDefaultConfig() (*sarama.Config, *cluster.Config) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.ClientID = "webstream_adapter"
	conf.Metadata.Retry.Max = 5
	conf.Metadata.Retry.Backoff = 1 * time.Second
	conf.Consumer.Return.Errors = true
	conf.Consumer.Retry.Backoff = 1 * time.Second
	conf.Consumer.Offsets.Retry.Max = 5
	conf.Admin.Timeout = 30 * time.Second // Not used
	clusConf := cluster.NewConfig()
	clusConf.Group.Return.Notifications = true
	clusConf.Group.Mode = cluster.ConsumerModeMultiplex
	clusConf.Consumer.Offsets.Initial = sarama.OffsetOldest
	clusConf.Config = *conf
	return conf, clusConf
}

func printf(format string, a ...interface{}) {
	str := fmt.Sprintf(format, a...)
	fmt.Printf("%s: %s\n", time.Now().Format(time.StampMilli), str)

}

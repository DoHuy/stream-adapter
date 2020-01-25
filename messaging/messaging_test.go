package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	messages "github.com/veritone/edge-messages"
	"github.com/veritone/edge-stream-ingestor/streamio"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	msgMocks "github.com/veritone/webstream-adapter/messaging/mocks"
)

func TestMessaging(t *testing.T) {
	suite.Run(t, new(messagingTestSuite))
	//suite.Run(t, new(kafkaTestSuite))
}

type messagingTestSuite struct {
	suite.Suite
	messageHelper *Helper
	messageClient *msgMocks.Client
}

func (t *messagingTestSuite) SetupTest() {
	var mcfg ClientConfig
	mctx := MessageContext{
		TaskID: "task1",
		JobID:  "job1",
		TDOID:  "tdo1",
	}
	t.messageClient = new(msgMocks.Client)
	t.messageHelper = NewHelper(t.messageClient, mcfg, mctx)
}

func (t *messagingTestSuite) TestInitMessageStreamWriter() {
	config := MessageStreamWriterConfig{
		Topic:          "stream_1",
		Partition:      int32(100),
		KeyPrefix:      "prefix__",
		MaxMessageSize: 1024 * 5,
		NoDataTimeout:  time.Second * 10,
	}

	sw := NewMessageStreamWriter(t.messageHelper, config)
	if assert.IsType(t.T(), new(messageWriter), sw) {
		writer := sw.(*messageWriter)
		assert.Equal(t.T(), config, writer.config)
		assert.NotNil(t.T(), writer.bytesWritten)
	}
}

func (t *messagingTestSuite) TestWriteStream() {
	ctx := context.Background()
	config := MessageStreamWriterConfig{
		Topic:          "stream_1",
		Partition:      int32(100),
		KeyPrefix:      "prefix__",
		MaxMessageSize: 1024,
	}
	sw := NewMessageStreamWriter(t.messageHelper, config)
	payload := make([]byte, 5*1024)
	stream := streamio.NewStreamFromReader(bytes.NewReader(payload))
	stream.MimeType = "video/mp4"
	stream.SetFfmpegFormat("mp4;vcodec=h264;acodec=aac")
	stream.StartOffsetMS = 20000
	stream.StartTime = time.Now()

	t.messageClient.On("ProduceWithPartition", ctx, config.Topic, config.Partition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	if assert.NoError(t.T(), sw.WriteStream(ctx, stream)) {
		// 7 calls: stream_init, 5 * raw_stream, stream_eof
		if t.messageClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", 7) {
			c := t.messageClient.Calls
			c[0].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__stream_init", mock.Anything)
			c[1].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[2].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[3].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[4].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[5].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[6].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__stream_eof", mock.Anything)

			// validate stream_init message
			valueArg, _ := c[0].Arguments.Get(4).([]byte)
			var streamInitMsg messages.StreamInit
			if assert.NoError(t.T(), json.Unmarshal(valueArg, &streamInitMsg)) {
				assert.Equal(t.T(), "task1", streamInitMsg.TaskID)
				assert.Equal(t.T(), "job1", streamInitMsg.JobID)
				assert.Equal(t.T(), "tdo1", streamInitMsg.TDOID)
				assert.Equal(t.T(), stream.StartOffsetMS, streamInitMsg.OffsetMS)
				assert.Equal(t.T(), stream.MimeType, streamInitMsg.MimeType)
				assert.Equal(t.T(), "mp4;vcodec=h264;acodec=aac", streamInitMsg.FfmpegFormat)
				assert.Equal(t.T(), stream.StartTime.Unix(), streamInitMsg.MediaStartTimeUTC)
			}

			// validate raw_stream message value
			for i := 1; i < 6; i++ {
				valueArg, _ := c[i].Arguments.Get(4).([]byte)
				start, end := (i-1)*1024, i*1024
				assert.Equal(t.T(), payload[start:end], valueArg)
			}

			// validate stream_eof message
			valueArg, _ = c[6].Arguments.Get(4).([]byte)
			var streamEOFMsg messages.StreamEOF
			if assert.NoError(t.T(), json.Unmarshal(valueArg, &streamEOFMsg)) {
				assert.Equal(t.T(), int64(5*1024), streamEOFMsg.ContentLength)
			}
		}
	}
}

func (t *messagingTestSuite) TestWriteStreamTimeout() {
	ctx := context.Background()
	config := MessageStreamWriterConfig{
		Topic:          "stream_1",
		Partition:      int32(100),
		MaxMessageSize: 1024,
		NoDataTimeout:  time.Millisecond * 250,
	}
	sw := NewMessageStreamWriter(t.messageHelper, config)
	stream := streamio.NewStream()

	t.messageClient.On("ProduceWithPartition", ctx, config.Topic, config.Partition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	err := sw.WriteStream(ctx, stream)
	if assert.Error(t.T(), err) {
		assert.Equal(t.T(), ErrWriterNoDataTimeout, err)
	}
}

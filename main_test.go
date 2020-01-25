package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	messages "github.com/veritone/edge-messages"
	"github.com/veritone/edge-stream-ingestor/streamio"
	streamMocks "github.com/veritone/edge-stream-ingestor/streamio/mocks"
	"github.com/veritone/webstream-adapter/api"
	apiMocks "github.com/veritone/webstream-adapter/api/mocks"
	"github.com/veritone/webstream-adapter/messaging"
	msgMocks "github.com/veritone/webstream-adapter/messaging/mocks"
	wsMocks "github.com/veritone/webstream-adapter/mocks"
	wsiMocks "github.com/veritone/webstream-adapter/mocks/mocks_internal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestWebstreamAdapter(t *testing.T) {
	suite.Run(t, new(mainTestSuite))
	suite.Run(t, new(ffmpegTestSuite))
	suite.Run(t, new(httpTestSuite))
	suite.Run(t, new(mpegDASHTestSuite))
}

type mainTestSuite struct {
	suite.Suite
	config       *engineConfig
	payload      *enginePayload
	client       *apiMocks.CoreAPIClient
	streamReader *wsMocks.Streamer
	streamWriter *streamMocks.StreamWriter
	kafkaClient  *msgMocks.Client
}

func (t *mainTestSuite) SetupTest() {
	t.config = &engineConfig{
		OutputTopicName:      "stream_buffer",
		OutputTopicPartition: 10,
		OutputTopicKeyPrefix: "task1__",
		Messaging: messaging.ClientConfig{
			MessageTopics: messaging.MessageTopicConfig{
				EngineStatus: "engine_status",
			},
		},
		FFMPEG: ffmpegConfig{
			HTTP: &ffmpegHTTPConfig{
				Reconnect: false,
			},
		},
	}
	t.config.defaults()

	sleepFunc = time.Sleep
	intialRetryInterval = time.Millisecond
	startTime := time.Now()
	endTime := startTime.Add(time.Second * 30)

	t.payload = &enginePayload{
		JobID:  "test-job-id",
		TaskID: "test-task-id",
		TDOID:  "test-tdo-id",
		taskPayload: taskPayload{
			RecordStartTime: startTime,
			RecordEndTime:   endTime,
			URL:             "http://stream.test",
		},
	}

	// mock kafka client
	t.kafkaClient = new(msgMocks.Client)

	// mock stream reader
	t.streamReader = new(wsMocks.Streamer)
	stream := streamio.NewStream()

	t.streamReader.On("Stream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("time.Duration")).Run(func(args mock.Arguments) {
		stream.StartTime = time.Now()
		stream.SendErr(nil)
	}).Return(stream)

	// mock stream writer
	t.streamWriter = new(streamMocks.StreamWriter)
	t.streamWriter.On("WriteStream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*streamio.Stream")).Run(func(args mock.Arguments) {
		stream := args.Get(1).(*streamio.Stream)
		io.Copy(ioutil.Discard, stream)
	}).Return(nil)

	// mock API client
	t.client = new(apiMocks.CoreAPIClient)
	t.client.On("FetchSource", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("string")).Return(new(api.Source), nil)

	// heartbeat
	hb = new(heartbeat)
}

func (t *mainTestSuite) TestPayloadValidate() {
	// current state is valid
	err := t.payload.validate()
	assert.NoError(t.T(), err)

	// no jobId
	pl := *t.payload
	pl.JobID = ""
	err = pl.validate()
	assert.Error(t.T(), err)

	// no taskId
	pl = *t.payload
	pl.TaskID = ""
	err = pl.validate()
	assert.Error(t.T(), err)

	// no tdoId
	pl = *t.payload
	pl.TDOID = ""
	err = pl.validate()
	assert.Error(t.T(), err)

	// no start or end time
	pl = *t.payload
	pl.RecordStartTime = time.Time{}
	pl.RecordEndTime = time.Time{}
	err = pl.validate()
	assert.NoError(t.T(), err)

	// start time but no end time
	pl = *t.payload
	pl.RecordEndTime = time.Time{}
	err = pl.validate()
	assert.Error(t.T(), err)

	// duration with start time
	pl.RecordDuration = "10s"
	err = pl.validate()
	assert.Error(t.T(), err)

	// duration without start/end time
	pl.RecordStartTime = time.Time{}
	err = pl.validate()
	assert.NoError(t.T(), err)

	// invalid duration
	pl.RecordDuration = "xyz"
	err = pl.validate()
	assert.Error(t.T(), err)

	// no URL
	pl = *t.payload
	pl.URL = ""
	err = pl.validate()
	assert.Error(t.T(), err)

	// no URL, but URL flag was provided
	*urlFlag = "some.url"
	pl.defaults()
	err = pl.validate()
	assert.NoError(t.T(), err)

	// no URL, but SourceID was provided
	*urlFlag = ""
	pl.URL = ""
	pl.SourceID = "12345"
	err = pl.validate()
	assert.NoError(t.T(), err)
}

func (t *mainTestSuite) TestPayloadValidateOfflineMode() {
	// test offline mode
	pl := *t.payload
	pl.CacheToS3Key = "abcd"
	assert.True(t.T(), pl.isOfflineMode())

	// should fail without required fields
	err := pl.validate()
	assert.Error(t.T(), err)

	pl.taskPayload.SourceID = "source1"
	err = pl.validate()
	assert.Error(t.T(), err)

	pl.taskPayload.ScheduledJobID = "schedJob1"
	err = pl.validate()
	assert.Error(t.T(), err)

	pl.taskPayload.SourceDetails = &api.SourceDetails{
		RadioStreamURL: "stream.com",
	}

	err = pl.validate()
	assert.NoError(t.T(), err)
}

func (t *mainTestSuite) TestIngestStream() {
	overrideTime := time.Now().UTC().Add(time.Hour * -2)
	t.payload.StartTimeOverride = overrideTime.Unix()
	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	errReason, _ := err.(errorReason)
	if t.NoError(errReason.error) {
		assert.NoError(t.T(), errReason.error)
	}
	assert.NoError(t.T(), nil)
	if t.streamReader.AssertCalled(t.T(), "Stream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("time.Duration")) {
		duration := t.streamReader.Calls[0].Arguments.Get(1).(time.Duration)
		expectedDur := t.payload.RecordEndTime.Sub(t.payload.RecordStartTime)
		// because of time elapsed since the payload was initialzed, the duration may be shorter
		assert.InDelta(t.T(), expectedDur.Seconds(), duration.Seconds(), 0.1)
	}

	if t.streamWriter.AssertCalled(t.T(), "WriteStream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*streamio.Stream")) {
		stream := t.streamWriter.Calls[0].Arguments.Get(1).(*streamio.Stream)
		if t.NotNil(stream) {
			t.Equal(overrideTime.Truncate(time.Second), stream.StartTime.Truncate(time.Second))
		}
	}
}

func (t *mainTestSuite) TestIngestStreamNoDuration() {
	t.payload.RecordStartTime = time.Time{}
	t.payload.RecordEndTime = time.Time{}

	err := IngestStream(context.Background(), t.payload, false, t.streamReader, t.streamWriter)
	errReason, _ := err.(errorReason)
	if t.NoError(errReason.error) {
		assert.NoError(t.T(), errReason.error)
	}
	if t.streamReader.AssertCalled(t.T(), "Stream", mock.AnythingOfType("*context.cancelCtx"), time.Duration(0)) {
		duration := t.streamReader.Calls[0].Arguments.Get(1).(time.Duration)
		assert.Equal(t.T(), 0.0, duration.Seconds())
	}

	t.streamWriter.AssertCalled(t.T(), "WriteStream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*streamio.Stream"))
}

func (t *mainTestSuite) TestIngestStreamLiveWithNoDuration() {
	t.payload.RecordStartTime = time.Time{}
	t.payload.RecordEndTime = time.Time{}

	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	assert.Error(t.T(), err)
}

func (t *mainTestSuite) TestIngestStreamMissedEndTime() {
	t.payload.RecordEndTime = time.Now()

	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	assert.EqualError(t.T(), err, errMissedRecordingWindow.Error())
}

func (t *mainTestSuite) TestIngestStreamLateStart() {
	t.payload.RecordStartTime = time.Now().Add(-10 * time.Second)
	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	errReason, _ := err.(errorReason)
	if t.NoError(errReason.error) {
		assert.NoError(t.T(), errReason.error)
	}

	if t.streamReader.AssertCalled(t.T(), "Stream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("time.Duration")) {
		duration := t.streamReader.Calls[0].Arguments.Get(1).(time.Duration)
		expectedDur := t.payload.RecordEndTime.Sub(time.Now())
		assert.InDelta(t.T(), expectedDur.Seconds(), duration.Seconds(), 0.1)
	}

	if t.streamWriter.AssertCalled(t.T(), "WriteStream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*streamio.Stream")) {
		stream := t.streamWriter.Calls[0].Arguments.Get(1).(*streamio.Stream)
		assert.Equal(t.T(), 0, stream.StartOffsetMS)
	}
}

func (t *mainTestSuite) TestIngestStreamLateStartWithOffset() {
	t.payload.RecordStartTime = time.Now().Add(-10 * time.Second)
	t.payload.TDOOffsetMS = 60000

	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	errReason, _ := err.(errorReason)
	if t.NoError(errReason.error) {
		assert.NoError(t.T(), errReason.error)
	}

	if t.streamReader.AssertCalled(t.T(), "Stream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("time.Duration")) {
		duration := t.streamReader.Calls[0].Arguments.Get(1).(time.Duration)
		expectedDur := t.payload.RecordEndTime.Sub(time.Now())
		assert.InDelta(t.T(), expectedDur.Seconds(), duration.Seconds(), 0.1)
	}

	if t.streamWriter.AssertCalled(t.T(), "WriteStream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*streamio.Stream")) {
		stream := t.streamWriter.Calls[0].Arguments.Get(1).(*streamio.Stream)
		assert.Equal(t.T(), 60000, stream.StartOffsetMS)
	}
}

func (t *mainTestSuite) TestIngestStreamEarlyStart() {
	var sleepTime time.Duration
	sleepFunc = func(d time.Duration) { sleepTime = d }

	// set to record 20 seconds from now
	t.payload.RecordStartTime = time.Now().Add(20 * time.Second)
	t.payload.TDOOffsetMS = 60000
	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	errReason, _ := err.(errorReason)
	if t.NoError(errReason.error) {
		assert.NoError(t.T(), errReason.error)
	}

	if t.streamReader.AssertCalled(t.T(), "Stream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("time.Duration")) {
		duration := t.streamReader.Calls[0].Arguments.Get(1).(time.Duration)
		expectedDur := t.payload.RecordEndTime.Sub(t.payload.RecordStartTime)
		assert.InDelta(t.T(), expectedDur.Seconds(), duration.Seconds(), 0.1)
		assert.InDelta(t.T(), 20.0, sleepTime.Seconds(), 0.1) // expected to sleep for 20 seconds
	}

	if t.streamWriter.AssertCalled(t.T(), "WriteStream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*streamio.Stream")) {
		stream := t.streamWriter.Calls[0].Arguments.Get(1).(*streamio.Stream)
		assert.Equal(t.T(), 60000, stream.StartOffsetMS)
	}
}

func (t *mainTestSuite) TestIngestStreamStreamErr() {
	streamErr := errors.New("stream failed")
	stream := streamio.NewStream()

	t.streamReader = new(wsMocks.Streamer)
	t.streamReader.On("Stream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("time.Duration")).Run(func(args mock.Arguments) {
		stream.SendErr(streamErr)
	}).Return(stream)

	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	errReason, ok := err.(errorReason)
	if t.True(ok) {
		if assert.Error(t.T(), errReason.error) {
			assert.Contains(t.T(), errReason.Error(), streamErr.Error(), errReason.failureReason)
		}
	}
}

func (t *mainTestSuite) TestIngestStreamStreamWriterErr() {
	writerErr := errors.New("writer failed")
	t.streamWriter = new(streamMocks.StreamWriter)
	t.streamWriter.On("WriteStream", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*streamio.Stream")).Return(writerErr)

	err := IngestStream(context.Background(), t.payload, true, t.streamReader, t.streamWriter)
	errReason, ok := err.(errorReason)
	if t.True(ok) {
		if assert.Error(t.T(), errReason.error) {
			assert.Contains(t.T(), errReason.Error(), writerErr.Error(), errReason.failureReason)
		}
	}
}

func (t *mainTestSuite) TestUrlHasFileExtension() {
	var imageFormats = []string{".jpg", ".png", ".jpeg", ".bmp", ".tiff", ".gif"}
	var imageURLToResult = map[string]bool{
		"http://example.com/image.jpg":            true,
		"http://example.com/image.png":            true,
		"http://example.com/image.jpeg":           true,
		"http://example.com/image.bmp":            true,
		"http://example.com/image.tiff":           true,
		"http://example.com/image.gif":            true,
		"http://example.com/image.mp4":            false,
		"http://example.com":                      false,
		"http://example.com/image.jpg?param=1234": true,
		"http://example.com/image.mp4?param=1234": false,
		"http://example.com/video.mjpeg":          false,
	}
	for k, v := range imageURLToResult {
		result := urlHasFileExtension(k, imageFormats...)
		assert.Equal(t.T(), v, result)
	}
}

func (t *mainTestSuite) TestInitAndRunLiveStream() {
	t.kafkaClient.On("Produce", mock.Anything, t.config.Messaging.MessageTopics.EngineStatus, mock.AnythingOfType("string"), mock.Anything).Return(nil)
	t.kafkaClient.On("ProduceWithPartition", mock.Anything, t.config.OutputTopicName, t.config.OutputTopicPartition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/white.mov"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{"video/quicktime"}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	t.payload.URL = srv.URL

	err = initAndRun(t.config, t.payload, "", t.kafkaClient, http.DefaultClient)
	if t.NoError(err) {
		stream, ok := hb.bytesReadTracker.(*streamio.Stream)
		if t.True(ok) {
			t.Equal(containerFormat, stream.FfmpegFormat)
		}

		numStreamMessages := 2 + int(math.Ceil(float64(hb.BytesWritten())/10240))
		if t.kafkaClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", numStreamMessages) {
			var produceWPCalls []mock.Call
			var lastHeartbeatMsg []byte

			for _, call := range t.kafkaClient.Calls {
				if call.Method == "ProduceWithPartition" {
					produceWPCalls = append(produceWPCalls, call)
				} else {
					lastHeartbeatMsg = call.Arguments.Get(3).([]byte)
				}
			}
			msg1KeyArg := produceWPCalls[0].Arguments.String(3)
			t.Equal("task1__stream_init", msg1KeyArg)

			msg2KeyArg := produceWPCalls[1].Arguments.String(3)
			t.Equal("task1__raw_stream", msg2KeyArg)

			lastMsgKeyArg := produceWPCalls[len(produceWPCalls)-1].Arguments.String(3)
			t.Equal("task1__stream_eof", lastMsgKeyArg)

			if t.NotEmpty(lastHeartbeatMsg) {
				var msg messages.EngineHeartbeat
				if t.NoError(json.Unmarshal(lastHeartbeatMsg, &msg)) {
					t.Equal(messages.EngineStatusDone, msg.Status)
					t.Equal(hb.BytesWritten(), msg.BytesWritten)
				}
			}
		}
	}
}

func (t *mainTestSuite) TestInitAndRunMediaFile() {
	t.kafkaClient.On("Produce", mock.Anything, t.config.Messaging.MessageTopics.EngineStatus, mock.AnythingOfType("string"), mock.Anything).Return(nil)
	t.kafkaClient.On("ProduceWithPartition", mock.Anything, t.config.OutputTopicName, t.config.OutputTopicPartition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/white.mov"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	transport := new(wsiMocks.RoundTripper)
	httpClient := &http.Client{
		Transport: transport,
	}

	transport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Return(&http.Response{
		Status:     "OK",
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"video/quicktime"}},
		Body:       ioutil.NopCloser(bytes.NewReader(b)),
	}, nil)

	t.payload.taskPayload = taskPayload{URL: "http://stream.test/file.mov"}

	err = initAndRun(t.config, t.payload, "", t.kafkaClient, httpClient)
	if t.NoError(err) {
		stream, ok := hb.bytesReadTracker.(*streamio.Stream)
		if t.True(ok) {
			t.Equal("video/quicktime", stream.MimeType)
		}

		numStreamMessages := 2 + int(math.Ceil(float64(len(b))/10240))
		if t.kafkaClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", numStreamMessages) {
			var produceWPCalls []mock.Call
			var lastHeartbeatMsg []byte

			for _, call := range t.kafkaClient.Calls {
				if call.Method == "ProduceWithPartition" {
					produceWPCalls = append(produceWPCalls, call)
				} else {
					lastHeartbeatMsg = call.Arguments.Get(3).([]byte)
				}
			}
			msg1KeyArg := produceWPCalls[0].Arguments.String(3)
			t.Equal("task1__stream_init", msg1KeyArg)

			msg2KeyArg := produceWPCalls[1].Arguments.String(3)
			t.Equal("task1__raw_stream", msg2KeyArg)

			lastMsgKeyArg := produceWPCalls[len(produceWPCalls)-1].Arguments.String(3)
			t.Equal("task1__stream_eof", lastMsgKeyArg)

			if t.NotEmpty(lastHeartbeatMsg) {
				var msg messages.EngineHeartbeat
				if t.NoError(json.Unmarshal(lastHeartbeatMsg, &msg)) {
					t.Equal(messages.EngineStatusDone, msg.Status)
					t.Equal(int64(len(b)), msg.BytesWritten)
					t.Equal(hb.BytesWritten(), msg.BytesWritten)
				}
			}
		}
	}
}

func (t *mainTestSuite) TestInitAndRunImage() {
	t.kafkaClient.On("Produce", mock.Anything, t.config.Messaging.MessageTopics.EngineStatus, mock.AnythingOfType("string"), mock.Anything).Return(nil)
	t.kafkaClient.On("ProduceWithPartition", mock.Anything, t.config.OutputTopicName, t.config.OutputTopicPartition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/image.png"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{"image/png"}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	t.payload.URL = srv.URL

	err = initAndRun(t.config, t.payload, "", t.kafkaClient, http.DefaultClient)
	errReason, _ := err.(errorReason)
	if t.NoError(errReason.error) {
		stream, ok := hb.bytesReadTracker.(*streamio.Stream)
		if t.True(ok) {
			t.Equal("image/png", stream.MimeType)
		}

		numStreamMessages := 2 + int(math.Ceil(float64(len(b))/10240))
		t.kafkaClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", numStreamMessages)

		var lastHeartbeatMsg []byte
		for _, call := range t.kafkaClient.Calls {
			if call.Method != "Produce" {
				continue
			}

			lastHeartbeatMsg = call.Arguments.Get(3).([]byte)
		}

		if t.NotEmpty(lastHeartbeatMsg) {
			var msg messages.EngineHeartbeat
			if t.NoError(json.Unmarshal(lastHeartbeatMsg, &msg)) {
				t.Equal(messages.EngineStatusDone, msg.Status)
				t.Equal(int64(len(b)), msg.BytesWritten)
			}
		}

		t.Equal(2, count) // 1 call to check content-type, 1 call for each stream attempt
	}
}

func (t *mainTestSuite) TestInitAndRunTextDocument() {
	t.kafkaClient.On("Produce", mock.Anything, t.config.Messaging.MessageTopics.EngineStatus, mock.AnythingOfType("string"), mock.Anything).Return(nil)
	t.kafkaClient.On("ProduceWithPartition", mock.Anything, t.config.OutputTopicName, t.config.OutputTopicPartition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	streamio.TextMimeTypes = []string{"application/pdf", "application/msword"}

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/document.pdf"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{"application/pdf"}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	t.payload.URL = srv.URL

	err = initAndRun(t.config, t.payload, "", t.kafkaClient, http.DefaultClient)
	if t.NoError(err) {
		stream, ok := hb.bytesReadTracker.(*streamio.Stream)
		if t.True(ok) {
			t.Equal("application/pdf", stream.MimeType)
		}

		numStreamMessages := 2 + int(math.Ceil(float64(len(b))/10240))
		t.kafkaClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", numStreamMessages)

		var lastHeartbeatMsg []byte
		for _, call := range t.kafkaClient.Calls {
			if call.Method != "Produce" {
				continue
			}

			lastHeartbeatMsg = call.Arguments.Get(3).([]byte)
		}

		if t.NotEmpty(lastHeartbeatMsg) {
			var msg messages.EngineHeartbeat
			if t.NoError(json.Unmarshal(lastHeartbeatMsg, &msg)) {
				t.Equal(messages.EngineStatusDone, msg.Status)
				t.Equal(int64(len(b)), msg.BytesWritten)
			}
		}

		t.Equal(2, count) // 1 call to check content-type, 1 call for each stream attempt
	}
}

func (t *mainTestSuite) TestInitAndRunRetry1stFailThenSucceeed() {
	t.kafkaClient.On("Produce", mock.Anything, t.config.Messaging.MessageTopics.EngineStatus, mock.AnythingOfType("string"), mock.Anything).Return(nil)
	t.kafkaClient.On("ProduceWithPartition", mock.Anything, t.config.OutputTopicName, t.config.OutputTopicPartition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/image.png"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write(nil)
		} else {
			w.WriteHeader(http.StatusOK)
			headers := w.Header()
			headers["Content-Type"] = []string{"image/png"}
			w.Write(b)
			w.Write(nil)
		}
		count++
	}))
	defer srv.Close()

	t.payload.URL = srv.URL

	err = initAndRun(t.config, t.payload, "", t.kafkaClient, http.DefaultClient)
	if t.NoError(err) {
		numStreamMessages := 2 + int(math.Ceil(float64(len(b))/10240))
		if t.kafkaClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", numStreamMessages) {
			var produceWPCalls []mock.Call
			var lastHeartbeatMsg []byte

			for _, call := range t.kafkaClient.Calls {
				if call.Method == "ProduceWithPartition" {
					produceWPCalls = append(produceWPCalls, call)
				} else {
					lastHeartbeatMsg = call.Arguments.Get(3).([]byte)
				}
			}
			msg1KeyArg := produceWPCalls[0].Arguments.String(3)
			t.Equal("task1__stream_init", msg1KeyArg)

			msg2KeyArg := produceWPCalls[1].Arguments.String(3)
			t.Equal("task1__raw_stream", msg2KeyArg)

			lastMsgKeyArg := produceWPCalls[len(produceWPCalls)-1].Arguments.String(3)
			t.Equal("task1__stream_eof", lastMsgKeyArg)

			if t.NotEmpty(lastHeartbeatMsg) {
				var msg messages.EngineHeartbeat
				if t.NoError(json.Unmarshal(lastHeartbeatMsg, &msg)) {
					t.Equal(messages.EngineStatusDone, msg.Status)
					t.Equal(hb.BytesWritten(), msg.BytesWritten)
				}
			}
		}

		t.Equal(3, count) // 1 call to check content-type, 1 call for each stream attempt
	}
}

func (t *mainTestSuite) TestInitAndRunFailAfterRetries() {
	t.kafkaClient.On("Produce", mock.Anything, t.config.Messaging.MessageTopics.EngineStatus, mock.AnythingOfType("string"), mock.Anything).Return(nil)
	t.kafkaClient.On("ProduceWithPartition", mock.Anything, t.config.OutputTopicName, t.config.OutputTopicPartition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/image.png"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write(nil)
		} else {
			// first attempt to get mime type is successful
			w.WriteHeader(http.StatusOK)
			headers := w.Header()
			headers["Content-Type"] = []string{"image/png"}
			w.Write(b)
			w.Write(nil)
		}
		count++
	}))
	defer srv.Close()

	t.payload.URL = srv.URL

	err = initAndRun(t.config, t.payload, "", t.kafkaClient, http.DefaultClient)
	errReason, ok := err.(errorReason)
	if t.True(ok) {
		t.Error(errReason.error)
		t.Contains(errReason.Error(), "503 Service Unavailable")
	}
	t.kafkaClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", 4)

	var lastHeartbeatMsg []byte
	for _, call := range t.kafkaClient.Calls {
		if call.Method != "Produce" {
			continue
		}

		lastHeartbeatMsg = call.Arguments.Get(3).([]byte)
	}

	if t.NotEmpty(lastHeartbeatMsg) {
		var msg messages.EngineHeartbeat
		if t.NoError(json.Unmarshal(lastHeartbeatMsg, &msg)) {
			t.Equal(messages.EngineStatusFailed, msg.Status)
			t.Equal(int64(0), msg.BytesWritten)
		}
	}

	t.Equal(6, count) // 1 call to check content-type, 1 call for each stream attempt
}

func (t *mainTestSuite) TestCheckMIMETypeTextDocument() {
	streamio.TextMimeTypes = []string{"application/pdf", "application/msword"}

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/document.pdf"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{"application/pdf"}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	_, isText, mimeType, err := checkMIMEType(context.Background(), srv.URL, *http.DefaultClient)
	if t.NoError(err) {
		t.True(isText)
		t.Equal("application/pdf", mimeType)
	}
}

func (t *mainTestSuite) TestWriteEmptyStream() {
	t.kafkaClient.On("ProduceWithPartition", mock.Anything, t.config.OutputTopicName, t.config.OutputTopicPartition, mock.AnythingOfType("string"), mock.Anything).Return(nil)

	msgr := messaging.NewHelper(t.kafkaClient, messaging.ClientConfig{}, messaging.MessageContext{})
	sw := messaging.NewMessageStreamWriter(msgr, messaging.MessageStreamWriterConfig{
		Topic:     t.config.OutputTopicName,
		Partition: t.config.OutputTopicPartition,
		KeyPrefix: t.config.OutputTopicKeyPrefix,
	})

	ctx := context.Background()
	err := writeEmptyStream(ctx, sw)
	if t.NoError(err) {
		if t.kafkaClient.AssertNumberOfCalls(t.T(), "ProduceWithPartition", 2) {
			for i, call := range t.kafkaClient.Calls {
				key := call.Arguments.String(3)
				if i == 0 {
					t.Equal("task1__stream_init", key)
				} else {
					t.Equal("task1__stream_eof", key)
				}
			}
		}
	}
}

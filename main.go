package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	messages "github.com/veritone/edge-messages"
	"github.com/veritone/edge-stream-ingestor/streamio"
	"github.com/veritone/realtime/modules/scfs"
	"github.com/veritone/webstream-adapter/api"
	"github.com/veritone/webstream-adapter/messaging"
)


const (
	appName     = "webstream-adapter"
	maxAttempts = 5
)

// Build flags set at build time (see Makefile)
var (
	BuildCommitHash string // BuildCommitHash is the commit hash of the build
	BuildTime       string // BuildTime is the time of the build
)

var (
	hb                       *heartbeat
	errMissedRecordingWindow = errors.New("we've overshot our recording window completely")
	errMissingEndTime        = errors.New("recording window must have an end time")
	sleepFunc                = time.Sleep
	intialRetryInterval      = time.Second
	rtspURLRegexp            = regexp.MustCompile("(?i)^rtsp:/")
	httpURLRegexp            = regexp.MustCompile("(?i)^https?:/")
)

// errorReason is the struct has failure reason and error
type errorReason struct {
	error
	failureReason messages.TaskFailureReason
}

// Streamer is the interface that wraps the Stream method
type Streamer interface {
	Stream(ctx context.Context, dur time.Duration) *streamio.Stream
}

func init() {
	streamio.Logger = log.New(os.Stderr, "[stream] ", log.LstdFlags)
}

func main() {
	// Log build info
	log.Printf("Build time: %s, Build commit hash: %s", BuildTime, BuildCommitHash)

	config, payload, err := loadConfigAndPayload()
	if err != nil {
		log.Fatal("Error initializing payload and config: ", err)
	}

	log.Printf("config=%s", config)
	log.Printf("payload=%s", payload)

	streamio.TextMimeTypes = config.SupportedTextMimeTypes

	// payload token overrides token from environment variable
	apiToken := os.Getenv("VERITONE_API_TOKEN")
	if payload.Token != "" {
		apiToken = payload.Token
	}

	config.Messaging.Kafka.ClientIDPrefix = appName + "_"
	kafkaClient, err := messaging.NewKafkaClient(config.Messaging.Kafka)
	if err != nil {
		log.Fatal(err)
	}
	defer kafkaClient.Close()

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: httpStreamerTCPTimeout}).DialContext,
			TLSHandshakeTimeout: httpStreamerTLSTimeout,
		},
	}
	if err := initAndRun(config, payload, apiToken, kafkaClient, httpClient); err != nil {
		log.Fatalf("TASK FAILED [%s]: %s", payload.TaskID, err)
	}

	log.Println("done")
}

func initAndRun(config *engineConfig, payload *enginePayload, apiToken string, kafkaClient messaging.Client, httpClient *http.Client) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create messaging helper - used for heartbeats and stream writer
	msgr := messaging.NewHelper(kafkaClient, config.Messaging, messaging.MessageContext{
		JobID:      payload.JobID,
		TaskID:     payload.TaskID,
		TDOID:      payload.TDOID,
		EngineID:   config.EngineID,
		InstanceID: config.EngineInstanceID,
	})

	// stream writer
	var sw streamio.StreamWriter
	var onCloseFn func(error, int64)

	// always run ScfsStreamWriter kham pha xem trong nay có cái gì
	scfsStreamWriter, err := scfs.InitScfsStreamWriter(strconv.FormatInt(payload.OrganizationID, 10), payload.JobID, payload.TaskID, payload.ScfsWriterBufferSize)

	if err != nil {
		return errorReason{
			err,
			messages.FailureReasonFileWriteError,
		}
	}

	sw = scfsStreamWriter

	if *stdoutFlag {
		// stdout stream writer dang tim hieu ve cac ham cua streamio
		sw = streamio.NewFileStreamWriter(os.Stdout)
	} else if payload.isOfflineMode() && !payload.DisableS3 {
		// when payload "CacheToS3Key" is available, webstream adapter is in "offline ingestion" mode
		cacheCfg := streamio.S3CacheConfig{
			Bucket:      config.OutputBucketName,
			Region:      config.OutputBucketRegion,
			MinioServer: config.MinioServer,
		}

		// callback called when the S3 writer has finished writing the file
		onFinishFn := func(url string, startTime int64, endTime int64) {
			msg := messages.EmptyOfflineIngestionRequest()
			msg.ScheduledJobID = payload.ScheduledJobID
			msg.SourceID = payload.SourceID
			msg.MediaURI = url
			msg.StartTime = startTime
			msg.EndTime = endTime
			msg.SourceTaskSummary.OrgID = strconv.FormatInt(payload.OrganizationID, 10)
			msg.SourceTaskSummary.JobID = payload.JobID
			msg.SourceTaskSummary.TaskID = payload.TaskID
			if hb != nil {
				msg.SourceTaskSummary.BytesRead = hb.BytesRead()
				msg.SourceTaskSummary.BytesWritten = hb.BytesWritten()
			}

			onCloseFn = func(err error, uptime int64) {
				// publish offline ingestion request message when done
				msg.SourceTaskSummary.UpTime = uptime
				if err != nil {
					msg.SourceTaskSummary.Status = messages.EngineStatusFailed
					msg.SourceTaskSummary.ErrorMsg = err.Error()
				} else {
					msg.SourceTaskSummary.Status = messages.EngineStatusDone
				}

				// send final message (msgr - package contain cai nay dung de gui msg len kafka)
				err = msgr.ProduceOffineIngestionRequestMessage(ctx, msg)
				if err == nil {
					log.Printf("Offline ingestion request message sent %+v:", msg)
				} else {
					log.Printf("Failed to send offline ingestion request message: %s", err)
				}
			}
		}

		log.Println("Enabling S3 Stream Writer")
		s3StreamWriter, err := NewS3StreamWriter(payload.CacheToS3Key, cacheCfg, onFinishFn)
		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		}

		sw, err = scfs.NewTeeStreamWriter(s3StreamWriter, scfsStreamWriter)
		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		}

	} else if !payload.DisableKafka {
		// sử dụng kafka
		log.Println("Enabling Kafka Stream Writer")
		kafkaStreamWriter := messaging.NewMessageStreamWriter(msgr, messaging.MessageStreamWriterConfig{
			Topic:     config.OutputTopicName,
			Partition: config.OutputTopicPartition,
			KeyPrefix: config.OutputTopicKeyPrefix,
		})

		sw, err = scfs.NewTeeStreamWriter(kafkaStreamWriter, scfsStreamWriter)
		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		}

		onCloseFn = func(err error, _ int64) {

			// ensure a stream EOF is always sent if an error occurred before writing the stream
			if err == nil || kafkaStreamWriter.BytesWritten() > 0 {
				return
			}

			// use a timeout in case the error was due to kafka failure
			sctx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()

			if err := writeEmptyStream(sctx, kafkaStreamWriter); err != nil {
				log.Println("failed to write empty stream:", err)
			}
		}
	}

	// start engine status heartbeats
	heartbeatInterval, _ := time.ParseDuration(config.HeartbeatInterval)

	// send heartbeat lien tuc kieu gi? khoi tao 1 routine send lien tuc theo time Duration, ket thuc bang ctx.Done()
	hb = startHeartbeat(ctx, msgr, heartbeatInterval, sw, payload.isOfflineMode())

	defer func() {
		if onCloseFn != nil {
			onCloseFn(err, hb.GetTaskUpTime())
		}
		// send final heartbeat
		status := messages.EngineStatusDone
		if err != nil {
			status = messages.EngineStatusFailed
		}

		log.Println("Sending final heartbeat. Status:", string(status))
		if err := hb.sendHeartbeat(ctx, status, err); err != nil {
			log.Println("Failed to send final heartbeat:", err)
		}
	}()

	// determine source URL
	var isLive bool
	url := payload.URL
	if url == "" {
		var sourceDetails api.SourceDetails
		if payload.SourceDetails != nil {
			sourceDetails = *payload.SourceDetails
			isLive = true // assume offline ingestion mode is used for live streams only
		} else {
			// fetch the URL from the Source configuration
			coreAPIClient, err := api.NewCoreAPIClient(config.VeritoneAPI, apiToken)
			if err != nil {
				return errorReason{
					fmt.Errorf("failed to initialize core API client: %s", err),
					messages.FailureReasonAPIError,
				}
			}

			source, err := coreAPIClient.FetchSource(ctx, payload.SourceID)
			if err != nil {
				return errorReason{
					fmt.Errorf("failed to fetch source %q: %s", payload.SourceID, err),
					messages.FailureReasonURLNotFound,
				}
			}

			sourceDetails = source.Details
			isLive = source.SourceType.IsLive
		}

		if sourceDetails.URL != "" {
			url = sourceDetails.URL
		} else if sourceDetails.RadioStreamURL != "" {
			url = sourceDetails.RadioStreamURL
		} else {
			return errorReason{
				fmt.Errorf("source %q does not have a URL field set", payload.SourceID),
				messages.FailureReasonAPINotFound,
			}
		}
	}

	isLive = isLive || payload.isTimeBased()
	log.Printf("Stream URL: %s Live: %v", url, isLive)

	// set up the stream reader stream reader cai nay khac vs sw stream writer
 	var sr Streamer

	switch true {
	case rtspURLRegexp.MatchString(url):
		sr = newRTSPReader(url)

	case httpURLRegexp.MatchString(url):
		// hack: provide JWT as Authorization header for media-streamer URLs
		if strings.Contains(url, "veritone") && strings.Contains(url, "/media-streamer/stream") {
			httpClient, err = api.NewAuthenticatedHTTPClient(config.VeritoneAPI, apiToken)
			if err != nil {
				return fmt.Errorf("http client err: %s", err)
			}
		}

		isImage, isText, mimeType, err := checkMIMEType(ctx, url, *httpClient)
		if err != nil && err != streamio.ErrUnknownMimeType {
			return errorReason{
				err,
				messages.FailureReasonInvalidData,
			}
		}
		if mimeType != "" {
			log.Println("Detected MIME Type:", mimeType)
		}
		if isImage || isText {
			log.Println("Using HTTP Reader.")
			sr = newHTTPStreamer(httpClient, url)
			break
		}
		if strings.HasPrefix(mimeType, mpjpegMimeType) ||
			urlHasFileExtension(url, ".mjpeg", ".mjpg", ".cgi") {
			log.Println("Using MJPEG Reader.")
			sr = newMJPEGReader(config.FFMPEG, url)
			break
		}
		if strings.HasPrefix(mimeType, mpegDASHMimeType) {
			// hack: streamlink has a bug where it doesn't return any valid streams when the DASH manifest
			// only has a single audio stream and no video. Clone the manifest on disk and insert a dummy
			// audio stream to work around this.
			url, err = patchMPD(ctx, url, httpClient)
			if err != nil {
				log.Println("failed to patch DASH manifest for streamlink:", err)
			}
		}
		// if not an image or supported text document, fall through to default (streamlink or ffmpeg)
		fallthrough

		// an dang phan van o cho return interface Streamer , tai sao phai lam nhu the
	default:
		if canStreamLinkHandleURL(ctx, url) {
			log.Println("Using StreamLink Reader.")
			sr = newStreamlinkReader(url)
		} else if isLive {
			log.Println("Using FFMPEG Reader.")
			sr = newFFMPEGReader(config.FFMPEG, url)
		} else {
			log.Println("Using HTTP Reader.")
			sr = newHTTPStreamer(httpClient, url)
		}
	}

	return IngestStream(ctx, payload, isLive, sr, sw)
}

// ham quan trong nhat
func IngestStream(ctx context.Context, payload *enginePayload, isLive bool, sr Streamer, sw streamio.StreamWriter) error {
	runTime := time.Now().UTC()
	recordStartTime := payload.RecordStartTime

	if recordStartTime.IsZero() {
		recordStartTime = runTime
	} else if runTime.After(recordStartTime) {
		log.Printf("We've overshot our recording window. Setting start time to %s.", runTime.Format(time.RFC3339))
		recordStartTime = runTime
	}

	var recordDuration time.Duration
	if payload.RecordDuration != "" {
		recordDuration, _ = time.ParseDuration(payload.RecordDuration)
	} else if !payload.RecordEndTime.IsZero() {
		recordDuration = payload.RecordEndTime.Sub(recordStartTime)
	} else if isLive {
		return errorReason{
			errMissingEndTime,
			messages.FailureReasonInvalidData,
		}
	}

	if isLive {
		if recordDuration <= 0 {
			return errorReason{
				errMissedRecordingWindow,
				messages.FailureReasonInvalidData,
			}
		}

		log.Printf("Recording stream %s from %s to %s.",
			payload.URL,
			recordStartTime.Format(time.RFC3339),
			recordStartTime.Add(recordDuration).Format(time.RFC3339))
	}

	// sleep until start time
	if now := time.Now().UTC(); now.Before(recordStartTime) {
		sleepTime := recordStartTime.Sub(now)
		log.Printf("Sleeping for %s", sleepTime.String())
		sleepFunc(sleepTime)
	}

	sctx, cancel := context.WithCancel(ctx)
	stream := sr.Stream(sctx, recordDuration)
	hb.trackReads(stream) // track stream progress in heartbeats

	// if payload specifies an offset in the TDO, pass it along in stream_init message
	if payload.TDOOffsetMS > 0 {
		stream.StartOffsetMS = payload.TDOOffsetMS
	}
	// if payload specifies a start time override, pass it along in stream_init message
	if payload.StartTimeOverride > 0 {
		stream.StartTime = time.Unix(payload.StartTimeOverride, 0).UTC()
	}

	errc := make(chan error, 2)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stream.Close()
		// listen for an error from stream reader
		if err := stream.ErrWait(); err != nil {
			log.Printf("stream reader err: %s", err)
			errc <- errorReason{
				fmt.Errorf("stream reader err: %s", err),
				messages.FailureReasonStreamReaderError,
			}
		}
	}()

	if err := sw.WriteStream(sctx, stream); err != nil {
		log.Printf("stream writer err: %s", err)
		errc <- errorReason{
			fmt.Errorf("stream writer err: %s", err),
			messages.FailureReasonFileWriteError,
		}
	}

	cancel() // cancel stream reader if it has not stopped already
	wg.Wait()
	close(errc)
	return <-errc
}

func urlHasFileExtension(urlStr string, suffixes ...string) bool {
	u, err := url.Parse(urlStr)
	if err != nil {
		log.Printf("Unable to parse url: %s, %s", urlStr, err)
		return false
	}
	for _, v := range suffixes {
		if strings.HasSuffix(u.Path, v) {
			return true
		}
	}
	return false
}

func checkMIMEType(ctx context.Context, urlStr string, httpClient http.Client) (bool, bool, string, error) {
	var stream *streamio.Stream
	var err error

	interval := intialRetryInterval
	httpClient.Timeout = time.Second * 15

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(interval)
			interval = exponentialIncrInterval(interval)
			log.Printf("RETRYING: ATTEMPT %d of %d", attempt, maxAttempts)
		}

		stream, err = tryDetermineMimeType(ctx, &httpClient, urlStr)
		if err == nil {
			break
		}

		log.Printf("failed to determine content type (attempt %d): %s", attempt, err)
	}

	// .IsText() checks the MIME type of the stream, but we also check file ext here just in case MIME isn't set
	isText := stream.IsText() ||
		urlHasFileExtension(urlStr, ".docx", ".doc", ".pdf", ".eml", ".msg", ".txt", ".ppt", ".rtf")

	return stream.IsImage(), isText, stream.MimeType, err
}

func writeEmptyStream(ctx context.Context, sw streamio.StreamWriter) error {
	stream := streamio.NewStream()
	stream.Close()
	return sw.WriteStream(ctx, stream)
}

type retryableError struct {
	error
}

func isErrRetryable(err error) bool {
	_, ok := err.(retryableError)
	return ok
}

func retryableErrorf(format string, args ...interface{}) error {
	return retryableError{fmt.Errorf(format, args...)}
}

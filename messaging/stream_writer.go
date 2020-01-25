package messaging

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	messages "github.com/veritone/edge-messages"
	"github.com/veritone/edge-stream-ingestor/streamio"
)

const (
	defaultStreamMessageSize = 1024 * 10
	defaultNoDataTimeout     = time.Minute
)

// ErrWriterNoDataTimeout is returned when the stream writer has not written any data in a certain timeout interval
var ErrWriterNoDataTimeout = fmt.Errorf("timeout: stream writer has not written any new data in %s", defaultNoDataTimeout)

type messageWriter struct {
	config       MessageStreamWriterConfig
	streamClient *Helper
	bytesWritten *streamio.Progress
}

// MessageStreamWriterConfig contains the options for configuring a message stream writer instance
type MessageStreamWriterConfig struct {
	Topic          string
	Partition      int32
	KeyPrefix      string
	MaxMessageSize int
	NoDataTimeout  time.Duration
}

func (c *MessageStreamWriterConfig) defaults() {
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = defaultStreamMessageSize
	}
	if c.NoDataTimeout == 0 {
		c.NoDataTimeout = defaultNoDataTimeout
	}
}

// NewMessageStreamWriter initializes and returns a new StreamWriter instance
func NewMessageStreamWriter(streamClient *Helper, config MessageStreamWriterConfig) streamio.StreamWriter {
	config.defaults()
	return &messageWriter{
		config:       config,
		streamClient: streamClient,
		bytesWritten: new(streamio.Progress),
	}
}

func (s *messageWriter) BytesWritten() int64 {
	return s.bytesWritten.Count()
}

func (s *messageWriter) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	md5Hash := md5.New()

	defer func() {
		// send stream EOF message
		log.Println("Sending stream EOF")
		eofMsg := messages.EmptyStreamEOF(s.config.KeyPrefix)
		eofMsg.ContentLength = s.bytesWritten.Count()
		eofMsg.MD5Checksum = hex.EncodeToString(md5Hash.Sum(nil))

		err := s.streamClient.ProduceStreamEOFMessage(ctx, s.config.Topic, s.config.Partition, eofMsg)
		if err != nil {
			log.Println("Failed to produce stream EOF message:", err)
		}
	}()

	// log progress
	go func() {
		defer log.Printf("-- %s written", s.bytesWritten)

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.Tick(time.Second):
				log.Printf("-- %s written", s.bytesWritten)
			}
		}
	}()

	// send stream_init message
	initMsg := messages.EmptyStreamInit(s.config.KeyPrefix)
	initMsg.OffsetMS = stream.StartOffsetMS
	initMsg.MediaStartTimeUTC = stream.StartTime.Unix()
	initMsg.MimeType = stream.MimeType
	initMsg.FfmpegFormat = stream.FfmpegFormatString()

	log.Println("Sending stream init message:")

	err := s.streamClient.ProduceStreamInitMessage(ctx, s.config.Topic, s.config.Partition, initMsg)
	if err != nil {
		return fmt.Errorf("failed to write stream init message: %s", err)
	}

	log.Printf("Init message: %+#v", initMsg)

	errc := make(chan error)
	writeTimeout := time.NewTimer(s.config.NoDataTimeout)
	defer writeTimeout.Stop()

	go func() {
		scanner := bufio.NewScanner(stream)
		scanner.Split(genSplitFunc(s.config.MaxMessageSize))

		for {
			if !scanner.Scan() {
				errc <- scanner.Err()
				return
			}

			// if a token was scanned, write it as a raw stream message
			if chunk := scanner.Bytes(); len(chunk) > 0 {
				msg := messages.EmptyRawStream(s.config.KeyPrefix)
				msg.Value = chunk
				err := s.streamClient.ProduceRawStreamMessage(ctx, s.config.Topic, s.config.Partition, &msg)
				if err != nil {
					errc <- fmt.Errorf("failed to write stream chunk: %s", err)
					return
				}

				s.bytesWritten.Write(chunk)
				md5Hash.Write(chunk)
				writeTimeout.Reset(s.config.NoDataTimeout)
			}
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-writeTimeout.C:
		return ErrWriterNoDataTimeout

	case err := <-errc:
		return err
	}
}

func genSplitFunc(numBytes int) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if len(data) >= numBytes {
			return numBytes, data[0:numBytes], nil
		}
		// If we're at EOF, we have a final token with less than numBytes bytes. Return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
}

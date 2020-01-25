package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/armon/circbuf"
	"github.com/veritone/edge-stream-ingestor/streamio"
)

const (
	containerMimeType = ""
	containerFormat   = "nut"
)

var defaultFfmpegHTTPCfg = ffmpegHTTPConfig{
	Reconnect:         false,
	ReconnectAtEOF:    true,
	ReconnectStreamed: true,
	ReconnectDelayMax: 5,
	Timeout:           5000000,
}

type ffmpegConfig struct {
	HTTP *ffmpegHTTPConfig `json:"http,omitempty"`
}

func (c *ffmpegConfig) defaults() {
	if c.HTTP == nil {
		c.HTTP = &defaultFfmpegHTTPCfg
	}
}

type ffmpegHTTPConfig struct {
	Reconnect         bool `json:"reconnect"`
	ReconnectAtEOF    bool `json:"reconnectAtEOF"`
	ReconnectStreamed bool `json:"reconnectStreamed"`
	ReconnectDelayMax int  `json:"reconnectDelayMax,omitempty"`
	Timeout           int  `json:"timeout,omitempty"`
}

func (c *ffmpegHTTPConfig) ToArgs() (args []string) {
	if c == nil {
		return
	}

	if c.Reconnect {
		args = []string{
			"-reconnect", "1",
		}
		if c.ReconnectAtEOF {
			args = append(args, "-reconnect_at_eof", "1")
		}
		if c.ReconnectStreamed {
			args = append(args, "-reconnect_streamed", "1")
		}
		if c.ReconnectDelayMax > 0 {
			args = append(args, "-reconnect_delay_max", strconv.Itoa(c.ReconnectDelayMax))
		}
	}

	if c.Timeout > 0 {
		args = append(args, "-timeout", strconv.Itoa(c.Timeout))
	}

	return
}

type ffmpegReader struct {
	config ffmpegConfig
	url    string
}

func newFFMPEGReader(config ffmpegConfig, url string) Streamer {
	config.defaults()
	return &ffmpegReader{
		config: config,
		url:    url,
	}
}

func (f *ffmpegReader) Stream(ctx context.Context, duration time.Duration) *streamio.Stream {
	// FFMPEG arguments
	var args []string

	// reconnect options are only available for http protocol
	if httpURLRegexp.MatchString(f.url) && f.config.HTTP != nil {
		args = f.config.HTTP.ToArgs()
	}

	args = append(args,
		"-i", f.url,
		"-c", "copy",
		"-f", containerFormat,
	)

	if duration > 0 {
		args = append(args, "-t", fmt.Sprintf("%.03f", duration.Seconds()))
	}

	stream := streamio.NewStream()
	stream.MimeType = containerMimeType
	stream.FfmpegFormat = containerFormat
	stream.StartTime = time.Now()

	go func() {
		var err error
		interval := intialRetryInterval

		for attempt := 0; attempt < maxAttempts; attempt++ {
			err = runFFMPEG(ctx, append(args, "-"), stream, nil)
			if err == nil || stream.BytesRead() > 0 {
				break
			}

			if attempt < maxAttempts {
				time.Sleep(interval)
				interval = exponentialIncrInterval(interval)
				log.Printf("RETRYING: ATTEMPT %d of %d", attempt+1, maxAttempts)
			}
		}

		stream.SendErr(err)
	}()

	return stream
}

func exponentialIncrInterval(interval time.Duration) time.Duration {
	return interval * 2
}

func runFFMPEG(ctx context.Context, args []string, stdout io.Writer, stdin io.Reader) error {
	log.Printf("FFMPEG %v", args)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = stdout
	if stdin != nil {
		cmd.Stdin = stdin
	}

	// write stderr stream to stdout and a buffer so errors can be parsed out
	stderrBuf, err := circbuf.NewBuffer(2 * 1024)
	if err != nil {
		return err
	}

	cmd.Stderr = stderrBuf

	if err := cmd.Run(); err != nil {
		log.Println("-------------------------------------------")
		log.Println("ffmpeg err:", err)
		log.Println("------------------output-------------------")
		log.Println(string(stderrBuf.Bytes()))
		log.Println("------------------output-------------------")
		err = parseFFMEGError(stderrBuf.Bytes(), err)
		return fmt.Errorf("ffmpeg error: %s", err)
	}

	return nil
}

func parseFFMEGError(stderr []byte, err error) error {
	s := string(stderr)
	if strings.Contains(s, "HTTP error 401") || strings.Contains(s, "Server returned 401") {
		return errors.New("HTTP error 401")
	} else if strings.Contains(s, "HTTP error 403") || strings.Contains(s, "Server returned 403") {
		return errors.New("HTTP error 403")
	} else if strings.Contains(s, "HTTP error 404") || strings.Contains(s, "Server returned 404") {
		return errors.New("HTTP error 404")
	} else if strings.Contains(s, "HTTP error 502") || strings.Contains(s, "Server returned 502") {
		return errors.New("HTTP error 502")
	} else if strings.Contains(s, "HTTP error 503") || strings.Contains(s, "Server returned 503") {
		return errors.New("HTTP error 503")
	} else if strings.Contains(s, "HTTP error 504") || strings.Contains(s, "Server returned 504") {
		return errors.New("HTTP error 504")
	} else if strings.Contains(s, "Connection timed out") {
		return errors.New("connection timed out")
	} else if strings.Contains(s, "Connection refused") {
		return errors.New("connection refused")
	} else if strings.Contains(s, "Connection reset by peer") {
		return errors.New("connection reset by peer")
	} else if strings.Contains(s, "Network is unreachable") {
		return errors.New("network is unreachable")
	} else if strings.Contains(s, "Name or service not known") {
		return errors.New("name or service not known")
	} else if strings.Contains(s, "Invalid data found when processing input") {
		return errors.New("invalid data found when processing input")
	}

	errText := err.Error()
	if errText == "exit status 1" {
		// scan to last line of stderr buffer and use for error message
		br := bytes.NewReader(stderr)
		scanner := bufio.NewScanner(br)
		for scanner.Scan() {
			errText = scanner.Text()
		}
	}

	return errors.New(errText)
}

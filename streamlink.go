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
	"time"

	"github.com/armon/circbuf"
	"github.com/veritone/edge-stream-ingestor/streamio"
)

type streamlinkReader struct {
	url string
}

func newStreamlinkReader(url string) Streamer {
	return &streamlinkReader{url}
}

func (f *streamlinkReader) Stream(ctx context.Context, duration time.Duration) *streamio.Stream {
	stream := streamio.NewStream()
	stream.StartTime = time.Now()

	if duration == 0 {
		go func() {
			err := runStreamlink(ctx, f.url, stream)
			stream.SendErr(err)
		}()

		probeOutput, err := streamio.ProbeStream(ctx, stream)
		if err != nil {
			stream.SendErr(err)
		}

		log.Printf("%+v", probeOutput)
		stream.FfmpegFormat = probeOutput.Format.FormatName
		return stream
	}

	stream.MimeType = containerMimeType
	stream.FfmpegFormat = containerFormat

	// pipe stdout of streamlink to stdin of ffmpeg
	pr, pw := io.Pipe()

	// run streamlink command
	go func() {
		if err := runStreamlink(ctx, f.url, pw); err != nil {
			stream.SendErr(err)
		}
	}()

	// run ffmpeg command
	go func() {
		// FFMPEG arguments
		args := []string{
			"-hide_banner",
			"-i", "pipe:0",
			"-c", "copy",
			"-f", containerFormat,
			"-t", fmt.Sprintf("%.03f", duration.Seconds()),
		}

		err := runFFMPEG(ctx, append(args, "-"), stream, pr)
		stream.SendErr(err)
	}()

	return stream
}

func runStreamlink(ctx context.Context, url string, stdout io.WriteCloser) error {
	defer stdout.Close()

	args := []string{
		"--stdout",         // stream to stdout
		"--retry-max", "5", // Max retries fetching the list of available streams until streams are found
		"--retry-streams", "1", // DELAY second(s) between each attempt
		"--retry-open", "5", // After a successful fetch, try ATTEMPTS time(s) to open the stream until giving up.
		url,
		"best", // choose "best" quality stream
	}

	log.Printf("streamlink %v", args)
	cmd := exec.CommandContext(ctx, "streamlink", args...)
	cmd.Stdout = stdout

	// write stderr stream to stdout and a buffer so errors can be parsed out
	stderrBuf, err := circbuf.NewBuffer(2 * 1024)
	if err != nil {
		return err
	}

	cmd.Stderr = stderrBuf

	if err := cmd.Run(); err != nil {
		log.Println("-------------------------------------------")
		log.Println("streamlink err:", err)
		log.Println("------------------output-------------------")
		log.Println(string(stderrBuf.Bytes()))
		log.Println("------------------output-------------------")

		if err.Error() != "signal: killed" {
			// scan to last line of stderr buffer and use for error message
			br := bytes.NewReader(stderrBuf.Bytes())
			scanner := bufio.NewScanner(br)
			for scanner.Scan() {
				err = errors.New(scanner.Text())
			}
		}

		return err
	}

	return nil
}

// check if Streamlink has a plugin that can handle the specified URL.
func canStreamLinkHandleURL(ctx context.Context, url string) bool {
	// Returns status code 1 for false and 0 for true
	cmd := exec.CommandContext(ctx, "streamlink", "--can-handle-url", url)
	return cmd.Run() == nil
}

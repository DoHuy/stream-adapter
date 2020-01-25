package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"regexp"
	"time"

	"github.com/veritone/edge-stream-ingestor/streamio"
)

const (
	defaultStreamExtension = "nut"
)

type s3StreamWriterCallback func(url string, startTime int64, endTime int64)

type s3StreamWriter struct {
	streamWriter streamio.Cacher
	bytesWritten *streamio.Progress
	destKey      string
	onFinish     s3StreamWriterCallback
}

func NewS3StreamWriter(destKey string, s3Config streamio.S3CacheConfig, onFinish s3StreamWriterCallback) (streamio.StreamWriter, error) {
	s3Cache, err := streamio.NewS3ChunkCache(s3Config)
	if err != nil {
		return nil, err
	}

	return &s3StreamWriter{
		streamWriter: s3Cache,
		onFinish:     onFinish,
		destKey:      fmt.Sprintf("%s.%s", destKey, defaultStreamExtension),
		bytesWritten: new(streamio.Progress)}, nil
}

func (s *s3StreamWriter) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	startTime := stream.StartTime.UTC().Unix() // actual stream start time
	endTime := startTime

	tr := io.TeeReader(stream, s.bytesWritten)
	url, err := s.streamWriter.Cache(ctx, tr, s.destKey, stream.MimeType)
	if err == nil {
		// determine actual media duration
		log.Println("Media cached to:", url)
		log.Println("Determining chunk duration...")

		duration, err := determineDuration(url)
		if err != nil {
			log.Println("Failed to determine media duration:", err)
		} else {
			log.Printf("Duration: %s", duration)
			endTime = stream.StartTime.Add(duration).UTC().Unix()
		}
	}

	if s.onFinish != nil {
		s.onFinish(url, startTime, endTime)
	}

	return err
}

func (s *s3StreamWriter) BytesWritten() int64 {
	return s.bytesWritten.Count()
}

func determineDuration(filename string) (time.Duration, error) {
	var duration time.Duration

	args := []string{
		"-hide_banner",
		"-i", filename,
		"-f", "null",
		"-",
	}

	cmd := exec.Command("ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return duration, err
	}

	re := regexp.MustCompile(` time=([0-9:.]+) `)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	log.Println(string(output))

	for scanner.Scan() {
		line := scanner.Text()

		if ok := re.MatchString(line); !ok {
			continue
		}

		matches := re.FindAllStringSubmatch(line, -1)
		if len(matches) == 0 {
			continue
		}

		subs := matches[len(matches)-1]
		if len(subs) < 2 {
			continue
		}

		timeBytes := []byte(subs[1])
		format := "03:04:05.000"

		if len(timeBytes) > len(format) {
			timeBytes = timeBytes[:len(format)] // truncate to length of format string
		}

		timeBytes = append(timeBytes, []byte(format)[len(timeBytes):]...) // pad remaining
		t, err := time.Parse(format, string(timeBytes))
		if err != nil {
			return duration, fmt.Errorf(`failed to parse duration string "%s" as time: %s`, string(timeBytes), err)
		}

		duration = time.Duration(t.Hour()) * time.Hour
		duration += time.Duration(t.Minute()) * time.Minute
		duration += time.Duration(t.Second()) * time.Second
		duration += time.Duration(t.Nanosecond()) * time.Nanosecond
	}

	return duration, nil
}

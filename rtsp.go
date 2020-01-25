package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/veritone/edge-stream-ingestor/streamio"
)

type rtspReader struct {
	url string
}

func newRTSPReader(url string) Streamer {
	return &rtspReader{url}
}

func (r *rtspReader) Stream(ctx context.Context, duration time.Duration) *streamio.Stream {
	// use different settings for rtsp streams, tcp tuning to make video less choppy
	args := []string{
		"-rtsp_transport", "tcp",
		"-i", r.url,
		"-tune", "zerolatency",
		"-t", fmt.Sprintf("%.0f", math.Ceil(duration.Seconds())),
		"-c", "copy",
		"-f", containerFormat,
		"-",
	}

	stream := streamio.NewStream()
	stream.MimeType = containerMimeType
	stream.FfmpegFormat = containerFormat
	stream.StartTime = time.Now()

	go func() {
		var err error
		interval := intialRetryInterval

		for attempt := 0; attempt < maxAttempts; attempt++ {
			err = runFFMPEG(ctx, args, stream, nil)
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

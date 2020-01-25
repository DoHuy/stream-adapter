package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/veritone/edge-stream-ingestor/streamio"
)

const mpjpegMimeType = "multipart/x-mixed-replace"

type mjpegReader struct {
	config ffmpegConfig
	url    string
}

func newMJPEGReader(config ffmpegConfig, url string) Streamer {
	config.defaults()

	return &mjpegReader{
		config: config,
		url:    url,
	}
}

func (r *mjpegReader) Stream(ctx context.Context, duration time.Duration) *streamio.Stream {
	// FFMPEG arguments
	var args []string

	// reconnect options are only available for http protocol
	if httpURLRegexp.MatchString(r.url) && r.config.HTTP != nil {
		args = r.config.HTTP.ToArgs()
	}

	// since the framerate of mjpeg streams cannot be accurately determined, use wallclock for frame timestamps
	args = append(args,
		"-use_wallclock_as_timestamps", "1",
		"-i", r.url,
		"-t", fmt.Sprintf("%.0f", math.Ceil(duration.Seconds())),
		"-an",
		"-c:v", "mjpeg",
		"-pix_fmt", "yuvj420p",
		"-f", containerFormat,
		"-",
	)

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

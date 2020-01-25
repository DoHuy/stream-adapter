package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/veritone/edge-stream-ingestor/streamio"
)

const (
	httpStreamerTCPTimeout = time.Second * 10
	httpStreamerTLSTimeout = time.Second * 10
)

type HTTPStreamer struct {
	httpClient *http.Client
	url        string
}

func newHTTPStreamer(httpClient *http.Client, url string) Streamer {
	return &HTTPStreamer{httpClient, url}
}

func (r *HTTPStreamer) Stream(ctx context.Context, dur time.Duration) *streamio.Stream {
	log.Println("streaming contents from", r.url)

	var stream *streamio.Stream
	var err error
	interval := intialRetryInterval

	for attempt := 0; attempt < maxAttempts; attempt++ {
		stream, err = getHTTPStream(ctx, r.httpClient, r.url)
		if err == nil || stream.BytesRead() > 0 || !isErrRetryable(err) {
			break
		}

		if attempt < maxAttempts {
			time.Sleep(interval)
			interval = exponentialIncrInterval(interval)
			log.Printf("RETRYING: ATTEMPT %d of %d", attempt+1, maxAttempts)
		}
	}

	stream.SendErr(err)
	return stream
}

func getHTTPStream(ctx context.Context, client *http.Client, url string) (*streamio.Stream, error) {
	stream := streamio.NewStream()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return stream, fmt.Errorf("unable to generate request for %q: %s", url, err)
	}

	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return stream, err
	}

	stream.OnClose(resp.Body.Close)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		if isStatusCodeRetryable(resp.StatusCode) {
			return stream, retryableErrorf("GET %s responded with: %s", url, resp.Status)
		}
		return stream, fmt.Errorf("GET %s responded with: %s", url, resp.Status)
	}

	stream = streamio.NewStreamFromReader(resp.Body)
	stream.MimeType = resp.Header.Get("Content-Type")
	stream.StartTime = time.Now()

	return stream, err
}

func tryDetermineMimeType(ctx context.Context, client *http.Client, urlStr string) (*streamio.Stream, error) {
	stream, err := getHTTPStream(ctx, client, urlStr)
	defer stream.Close()
	if err != nil {
		if _, ok := err.(*url.Error); ok {
			// url.Error is returned by client.Do when there is a failure to speak HTTP. Just return nil error
			return stream, nil
		}
		return stream, err
	}

	if stream.MimeType == "" {
		err = stream.GuessMimeType()
		if err == streamio.ErrUnknownMimeType {
			return stream, nil
		}
	}

	return stream, err
}

func isStatusCodeRetryable(status int) bool {
	return status < 400 || status >= 500 || status == 429
}

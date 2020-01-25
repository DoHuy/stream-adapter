package main

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/stretchr/testify/suite"
)

type httpTestSuite struct {
	suite.Suite
}

func (t *httpTestSuite) SetupTest() {
	intialRetryInterval = time.Millisecond
}

func (t *httpTestSuite) TestStreamFailDueToNonRetryableError() {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	reader := newHTTPStreamer(http.DefaultClient, srv.URL)
	stream := reader.Stream(context.Background(), 1*time.Second)
	if t.NotNil(stream) {
		go io.Copy(ioutil.Discard, stream)
		err := stream.ErrWait()
		if t.Error(err) {
			t.Contains(err.Error(), "400 Bad Request")
		}
	}
	t.Equal(1, count)
}

func (t *httpTestSuite) TestStreamFailDueToServiceUnavailable() {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	reader := newHTTPStreamer(http.DefaultClient, srv.URL)
	stream := reader.Stream(context.Background(), 1*time.Second)
	if t.NotNil(stream) {
		go io.Copy(ioutil.Discard, stream)
		err := stream.ErrWait()
		if t.Error(err) {
			t.Contains(err.Error(), "503 Service Unavailable")
		}
	}

	t.Equal(maxAttempts, count)
}

func (t *httpTestSuite) TestStreamFailDueToTCPConnectionTimeout() {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
	}))
	defer srv.Close()

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: time.Nanosecond}).DialContext,
		},
	}
	reader := newHTTPStreamer(httpClient, srv.URL)
	stream := reader.Stream(context.Background(), 1*time.Second)
	if t.NotNil(stream) {
		go io.Copy(ioutil.Discard, stream)
		err := stream.ErrWait()
		if t.Error(err) {
			t.Contains(err.Error(), `i/o timeout`)
		}
	}

	t.Equal(0, count)
}

func (t *httpTestSuite) TestStreamFailDueToTLSConnectionTimeout() {
	count := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
	}))
	defer srv.Close()

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout: time.Nanosecond,
		},
	}
	reader := newHTTPStreamer(httpClient, srv.URL)
	stream := reader.Stream(context.Background(), 1*time.Second)
	if t.NotNil(stream) {
		go io.Copy(ioutil.Discard, stream)
		err := stream.ErrWait()
		if t.Error(err) {
			t.Contains(err.Error(), `TLS handshake timeout`)
		}
	}

	t.Equal(0, count)
}

func (t *httpTestSuite) TestStreamSuccess() {
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

	reader := newHTTPStreamer(http.DefaultClient, srv.URL)
	stream := reader.Stream(context.Background(), time.Duration(0))
	if t.NotNil(stream) {
		go func() {
			n, err := io.Copy(ioutil.Discard, stream)
			t.NoError(err)
			t.True(n > 0)
		}()
		t.NoError(stream.ErrWait())
		t.Equal("", stream.FfmpegFormat)
		t.Equal("image/png", stream.MimeType)
		t.InDelta(time.Now().Unix(), stream.StartTime.Unix(), 1)
	}

	t.Equal(1, count)
}

func (t *httpTestSuite) TestStreamRetry1stFailThenSucceeed() {
	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/image.png"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 {
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

	reader := newHTTPStreamer(http.DefaultClient, srv.URL)
	stream := reader.Stream(context.Background(), time.Duration(0))
	if t.NotNil(stream) {
		go func() {
			n, err := io.Copy(ioutil.Discard, stream)
			t.NoError(err)
			t.True(n > 0)
		}()
		t.NoError(stream.ErrWait())
		t.Equal("", stream.FfmpegFormat)
		t.Equal("image/png", stream.MimeType)
		t.InDelta(time.Now().Unix(), stream.StartTime.Unix(), 1)
	}

	t.Equal(2, count)
}

func (t *httpTestSuite) TestIsStatusCodeRetryable() {
	t.Equal(true, isStatusCodeRetryable(399))
	t.Equal(true, isStatusCodeRetryable(500))
	t.Equal(true, isStatusCodeRetryable(429))
	t.Equal(true, isStatusCodeRetryable(299))
	t.Equal(true, isStatusCodeRetryable(200))
	t.Equal(false, isStatusCodeRetryable(400))
	t.Equal(false, isStatusCodeRetryable(401))
	t.Equal(false, isStatusCodeRetryable(499))
}

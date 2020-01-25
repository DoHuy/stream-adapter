package main

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/veritone/webstream-adapter/api"
)

type mpegDASHTestSuite struct {
	suite.Suite
	sampleAudioMPD string
	sampleVideoMPD string
}

func (t *mpegDASHTestSuite) SetupTest() {
	intialRetryInterval = time.Millisecond
	cwd, _ := os.Getwd()
	t.sampleAudioMPD = cwd + "/sample/dash_audio.mpd"
	t.sampleVideoMPD = cwd + "/sample/dash_video.mpd"
}

func (t *mpegDASHTestSuite) TestPatchMPD() {
	var authHeader string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// capture Authorization header
		if h, ok := r.Header["Authorization"]; ok && len(h) > 0 {
			authHeader = h[0]
		}

		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{mpegDASHMimeType}
		b, err := ioutil.ReadFile(t.sampleAudioMPD)
		if err != nil {
			t.Failf("failed to read sample file %q: %s", t.sampleAudioMPD, err)
		}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	ctx := context.Background()
	httpClient, _ := api.NewAuthenticatedHTTPClient(api.Config{}, "i_am_a_token")
	newURL, err := patchMPD(ctx, srv.URL, httpClient)
	if t.NoError(err) {
		if t.Contains(newURL, "file://") {
			filePath := strings.TrimPrefix(newURL, "file://")
			contents, err := ioutil.ReadFile(filePath)
			if t.NoError(err) {
				t.Contains(string(contents), `<Representation id="0" mimeType="audio/mp4" codecs="mp4a.40.2" bandwidth="100000">`)
				t.Contains(string(contents), `<Representation id="1" mimeType="audio/mp4" codecs="mp4a.40.2" bandwidth="0">`)
			}
		}
	}

	t.Equal("Bearer i_am_a_token", authHeader)
}

func (t *mpegDASHTestSuite) TestPatchMPDWithVideo() {
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{mpegDASHMimeType}
		b, err := ioutil.ReadFile(t.sampleVideoMPD)
		if err != nil {
			t.Failf("failed to read sample file %q: %s", t.sampleVideoMPD, err)
		}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	newURL, err := patchMPD(ctx, srv.URL, http.DefaultClient)
	if t.NoError(err) {
		if t.Contains(newURL, "file://") {
			filePath := strings.TrimPrefix(newURL, "file://")
			contents, err := ioutil.ReadFile(filePath)
			if t.NoError(err) {
				t.NotContains(string(contents), `duration="PT2M52.572S"`)
			}
		}
	}
}

func (t *mpegDASHTestSuite) TestPatchMPDRetryAndFail() {
	ctx := context.Background()
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write(nil)
		count++
	}))
	defer srv.Close()

	newURL, err := patchMPD(ctx, srv.URL, http.DefaultClient)
	t.Error(err)
	t.Equal(maxAttempts, count)
	t.Equal(srv.URL, newURL)
}

func (t *mpegDASHTestSuite) TestPatchMPDRetryAndSucceed() {
	ctx := context.Background()
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count > 0 {
			w.WriteHeader(http.StatusOK)
			headers := w.Header()
			headers["Content-Type"] = []string{mpegDASHMimeType}
			b, err := ioutil.ReadFile(t.sampleAudioMPD)
			if err != nil {
				t.Failf("failed to read sample file %q: %s", t.sampleAudioMPD, err)
			}
			w.Write(b)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		w.Write(nil)
		count++
	}))
	defer srv.Close()

	newURL, err := patchMPD(ctx, srv.URL, http.DefaultClient)
	t.NoError(err)
	t.Contains(newURL, "file://")
}

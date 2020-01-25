package main

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/stretchr/testify/suite"
)

type ffmpegTestSuite struct {
	suite.Suite
	ffmpegConfig ffmpegConfig
}

func (t *ffmpegTestSuite) SetupTest() {
	intialRetryInterval = time.Millisecond
	t.ffmpegConfig = ffmpegConfig{
		HTTP: &ffmpegHTTPConfig{
			Reconnect: false, // reconnects breaks the unit tests when run in docker
		},
	}
}

func (t *ffmpegTestSuite) TestStreamFail() {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	reader := newFFMPEGReader(t.ffmpegConfig, srv.URL)
	stream := reader.Stream(context.Background(), 1*time.Second)
	if t.NotNil(stream) {
		go io.Copy(ioutil.Discard, stream)
		err := stream.ErrWait()
		if t.Error(err) {
			t.Contains(err.Error(), "HTTP error 503")
		}
	}

	t.Equal(maxAttempts, count)
}

func (t *ffmpegTestSuite) TestStreamSuccess() {
	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/white.mov"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{"video/quicktime"}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	reader := newFFMPEGReader(t.ffmpegConfig, srv.URL)
	stream := reader.Stream(context.Background(), time.Duration(0))
	if t.NotNil(stream) {
		go func() {
			n, err := io.Copy(ioutil.Discard, stream)
			t.NoError(err)
			t.True(n > 0)
		}()

		t.NoError(stream.ErrWait())
		t.Equal(containerFormat, stream.FfmpegFormat)
		t.Equal(containerMimeType, stream.MimeType)
		t.InDelta(time.Now().Unix(), stream.StartTime.Unix(), 1)
	}

	t.Equal(1, count)
}

func (t *ffmpegTestSuite) TestStreamRetry1stFailThenSucceeed() {
	cwd, _ := os.Getwd()
	sampleFile := cwd + "/sample/white.mov"
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
			headers["Content-Type"] = []string{"video/quicktime"}
			w.Write(b)
			w.Write(nil)
		}
		count++
	}))
	defer srv.Close()

	reader := newFFMPEGReader(t.ffmpegConfig, srv.URL)
	stream := reader.Stream(context.Background(), time.Duration(0))
	if t.NotNil(stream) {
		go func() {
			n, err := io.Copy(ioutil.Discard, stream)
			t.NoError(err)
			t.True(n > 0)
		}()

		t.NoError(stream.ErrWait())
		t.Equal(containerFormat, stream.FfmpegFormat)
		t.Equal(containerMimeType, stream.MimeType)
		t.InDelta(time.Now().Unix(), stream.StartTime.Unix(), 1)
	}

	t.Equal(2, count)
}

// func (t *ffmpegTestSuite) TestStreamFailAfterCaptureSomeDataNoRetry() {
// 	cwd, _ := os.Getwd()
// 	sampleFile := cwd + "/sample/white.mov"
// 	b, err := ioutil.ReadFile(sampleFile)
// 	if err != nil {
// 		t.Failf("failed to read sample file %q: %s", sampleFile, err)
// 	}

// 	count := 0
// 	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		w.WriteHeader(http.StatusOK)
// 		headers := w.Header()
// 		headers["Content-Type"] = []string{"video/quicktime"}
// 		if count == 0 {
// 			// write some invalid data
// 			w.Write(b)
// 			w.Write([]byte("abcd"))
// 			w.Write(nil)
// 		} else {
// 			w.Write(b)
// 			w.Write(nil)
// 		}
// 		count++
// 	}))
// 	defer srv.Close()

// 	reader := newFFMPEGReader(t.ffmpegConfig, srv.URL)
// 	stream := reader.Stream(context.Background(), time.Duration(0))
// 	if t.NotNil(stream) {
// 		go func() {
// 			n, err := io.Copy(ioutil.Discard, stream)
// 			t.NoError(err)
// 			t.True(n > 0)
// 		}()

// 		if err := stream.ErrWait(); t.Error(err) {
// 			t.Contains(err.Error(), "Invalid data found when processing input")
// 		}
// 	}

// 	t.Equal(1, count)
// }

func (t *ffmpegTestSuite) TestParseFFMPEGError() {
	stderr := []byte(`ffmpeg version 4.0.1 Copyright (c) 2000-2018 the FFmpeg developers
built with gcc 6.2.1 (Alpine 6.2.1) 20160822
configuration: --disable-debug --disable-doc --disable-ffplay --enable-shared --enable-avresample --enable-libopencore-amrnb --enable-libopencore-amrwb --enable-gpl --enable-libass --enable-libfreetype --enable-libvidstab --enable-libmp3lame --enable-libopenjpeg --enable-libopus --enable-libtheora --enable-libvorbis --enable-libvpx --enable-libx265 --enable-libxvid --enable-libx264 --enable-nonfree --enable-openssl --enable-libfdk_aac --enable-libkvazaar --enable-libaom --extra-libs=-lpthread --enable-postproc --enable-small --enable-version3 --extra-cflags=-I/opt/ffmpeg/include --extra-ldflags=-L/opt/ffmpeg/lib --extra-libs=-ldl --prefix=/opt/ffmpeg
libavutil 56. 14.100 / 56. 14.100
libavcodec 58. 18.100 / 58. 18.100
libavformat 58. 12.100 / 58. 12.100
libavdevice 58. 3.100 / 58. 3.100
libavfilter 7. 16.100 / 7. 16.100
libavresample 4. 0. 0 / 4. 0. 0
libswscale 5. 1.100 / 5. 1.100
libswresample 3. 1.100 / 3. 1.100
libpostproc 55. 1.100 / 55. 1.100
http://54.82.82.169/entercom-kjceamaac-64: Operation timed out`)

	err := parseFFMEGError(stderr, errors.New("exit status 1"))
	if t.Error(err) {
		t.Equal("http://54.82.82.169/entercom-kjceamaac-64: Operation timed out", err.Error())
	}
}

package main

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/veritone/webstream-adapter/mpd"
)

const mpegDASHMimeType = "application/dash+xml"

func patchMPD(ctx context.Context, url string, httpClient *http.Client) (string, error) {
	var m *mpd.MPD
	var err error
	interval := intialRetryInterval

	for attempt := 0; attempt < maxAttempts; attempt++ {
		m, err = downloadAndParseMPD(ctx, httpClient, url)
		if err == nil {
			break
		}

		if attempt < maxAttempts {
			time.Sleep(interval)
			interval = exponentialIncrInterval(interval)
			log.Printf("RETRYING: ATTEMPT %d of %d", attempt+1, maxAttempts)
		}
	}

	if err != nil {
		return url, err
	}

	if hasOnlySingleAudioStream(m) {
		log.Println("DASH mpd only contains a single audio stream. Adding dummy stream.")

		// add dummy Representation element with 0 bandwidth
		d := m.Period.AdaptationSets[0].Representations[0]
		d.ID = new(string)
		*d.ID = "1"
		d.Bandwidth = new(uint64)
		m.Period.AdaptationSets[0].Representations = append(m.Period.AdaptationSets[0].Representations, d)
	}

	mpdXML, err := m.Encode()
	if err != nil {
		return url, err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return url, err
	}

	mpdPath := cwd + "/dash.mpd"
	mpdFile, err := os.OpenFile(mpdPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return url, err
	}
	defer mpdFile.Chdir()

	if _, err := mpdFile.Write(mpdXML); err != nil {
		return url, err
	}

	return "file://" + mpdFile.Name(), nil
}

func downloadAndParseMPD(ctx context.Context, httpClient *http.Client, url string) (*mpd.MPD, error) {
	stream, err := getHTTPStream(ctx, httpClient, url)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	mpdBytes, err := ioutil.ReadAll(stream)
	if err != nil {
		return nil, err
	}

	m := new(mpd.MPD)
	return m, m.Decode(mpdBytes)
}

func hasOnlySingleAudioStream(m *mpd.MPD) bool {
	if m.Period == nil || len(m.Period.AdaptationSets) != 1 {
		return false
	}

	as := m.Period.AdaptationSets[0]
	if len(as.Representations) != 1 {
		return false
	}

	return as.Representations[0].MimeType != nil && strings.HasPrefix(*as.Representations[0].MimeType, "audio")
}

package api

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"time"
)

func NewAuthenticatedHTTPClient(config Config, token string) (*http.Client, error) {
	config.defaults()
	timeoutDuration, err := time.ParseDuration(config.Timeout)
	if err != nil {
		return nil, fmt.Errorf(`invalid timeout value "%s" - %s`, config.Timeout, err)
	}

	return httpClientWithAuthHeader(token, config.CorrelationID, timeoutDuration, config.Debug), nil
}

func httpClientWithAuthHeader(token string, correlationID string, timeout time.Duration, debug bool) *http.Client {
	tr := &authHTTPTransport{
		RoundTripper:  http.DefaultTransport,
		token:         token,
		correlationID: correlationID,
		debug:         debug,
	}

	return &http.Client{Transport: tr, Timeout: timeout}
}

type authHTTPTransport struct {
	http.RoundTripper
	token         string
	correlationID string
	debug         bool
}

func (t *authHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	req.Header.Set("Veritone-Correlation-ID", t.correlationID)
	if t.debug {
		dump, _ := httputil.DumpRequest(req, true)
		log.Printf("%q", dump)
	}
	return t.RoundTripper.RoundTrip(req)
}

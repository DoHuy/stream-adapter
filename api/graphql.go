package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/veritone/graphql"
)

const (
	defaultBaseURL         = "https://api.veritone.com"
	defaultGraphQLEndpoint = "/v3/graphql"
	defaultTimeout         = "30s"
)

type CoreAPIClient interface {
	FetchSource(ctx context.Context, sourceID string) (*Source, error)
}

type Config struct {
	BaseURL         string `json:"baseURL,omitempty"`
	GraphQLEndpoint string `json:"graphQLEndpoint,omitempty"`
	Timeout         string `json:"timeout,omitempty"`
	Debug           bool   `json:"debug"`
	CorrelationID   string `json:"correlationID,omitempty"`
}

func (c *Config) defaults() {
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	if c.GraphQLEndpoint == "" {
		c.GraphQLEndpoint = defaultGraphQLEndpoint
	}
	if c.Timeout == "" {
		c.Timeout = defaultTimeout
	}
}

func beforeRetryHandler(req *http.Request, resp *http.Response, err error, num int) {
	if err != nil {
		log.Printf("Retrying (attempt %d) after err: %s -- request: %+v response: %+v", num, err, req, resp)
	} else {
		log.Printf("Retrying (attempt %d) after status: %s -- request: %+v response: %+v", num, resp.Status, req, resp)
	}
}

func NewCoreAPIClient(config Config, token string) (CoreAPIClient, error) {
	config.defaults()
	endpoint := config.BaseURL + config.GraphQLEndpoint

	timeoutDuration, err := time.ParseDuration(config.Timeout)
	if err != nil {
		return nil, fmt.Errorf(`invalid timeout value "%s" - %s`, config.Timeout, err)
	}

	httpClient := httpClientWithAuthHeader(token, config.CorrelationID, timeoutDuration, config.Debug)
	baseClient := graphql.NewClient(endpoint,
		graphql.WithHTTPClient(httpClient),
		graphql.WithDefaultExponentialRetryConfig(),
		graphql.WithBeforeRetryHandler(beforeRetryHandler))

	if config.Debug {
		baseClient.Log = func(s string) { log.Println(s) }
	}

	return &graphQLClient{Client: baseClient}, nil
}

type graphQLClient struct {
	*graphql.Client
}

func (c *graphQLClient) FetchSource(ctx context.Context, sourceID string) (*Source, error) {
	req := graphql.NewRequest(`
		query($id: ID!) {
			source(id: $id) {
				id
				sourceType {
					isLive
				}
				details
			}
		}`)

	req.Var("id", sourceID)

	var resp struct {
		Result *Source `json:"source"`
	}

	return resp.Result, c.Run(ctx, req, &resp)
}

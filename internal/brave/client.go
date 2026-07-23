package brave

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const defaultBaseURL = "https://api.search.brave.com"

// Result is a single web search result from Brave Search.
type Result struct {
	Title       string `json:"title"`
	URL         string `json:"url"`
	Description string `json:"description"`
	Age         string `json:"age"`
}

// Response is the top-level API response from Brave Search.
type Response struct {
	Web struct {
		Results []Result `json:"results"`
	} `json:"web"`
}

// Client defines the interface for searching Brave Search.
type Client interface {
	Search(ctx context.Context, query string) (*Response, error)
}

type httpClient struct {
	apiKey  string
	baseURL string
	http    *http.Client
}

// New creates a new Brave Search client with the given API key and HTTP timeout.
func New(apiKey string, timeout time.Duration) Client {
	return &httpClient{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// newAtURL swaps the base URL of the client — for tests only.
func newAtURL(c Client, baseURL string) Client {
	if hc, ok := c.(*httpClient); ok {
		hc.baseURL = baseURL
	}
	return c
}

func (c *httpClient) Search(ctx context.Context, query string) (*Response, error) {
	u := fmt.Sprintf("%s/res/v1/web/search?q=%s", c.baseURL, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Subscription-Token", c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("brave: status %d", resp.StatusCode)
	}

	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

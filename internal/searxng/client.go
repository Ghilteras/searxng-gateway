package searxng

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Result represents a single search result from SearXNG.
type Result struct {
	Title         string  `json:"title"`
	URL           string  `json:"url"`
	Content       string  `json:"content"`
	Engine        string  `json:"engine"`
	Score         float64 `json:"score"`
	PublishedDate string  `json:"publishedDate,omitempty"`
}

// Response is the top-level API response from SearXNG.
type Response struct {
	Results []Result `json:"results"`
}

// Client defines the interface for searching SearXNG.
type Client interface {
	Search(ctx context.Context, query string) (*Response, error)
}

type httpClient struct {
	baseURL string
	http    *http.Client
}

// New creates a new SearXNG client with the given base URL and HTTP timeout.
func New(baseURL string, timeout time.Duration) Client {
	return &httpClient{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

func (c *httpClient) Search(ctx context.Context, query string) (*Response, error) {
	u := fmt.Sprintf("%s/search?q=%s&format=json", c.baseURL, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("searxng: status %d", resp.StatusCode)
	}
	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

package brave

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"sx/internal/metrics"
)

const defaultBaseURL = "https://api.search.brave.com"

// Result is a single web search result from Brave Search.
type Result struct {
	Title       string `json:"title"`
	URL         string `json:"url"`
	Description string `json:"description"`
	Age         string `json:"age"`
}

// RateLimit holds the parsed X-RateLimit-* headers from Brave API responses.
type RateLimit struct {
	LimitMonth     float64 // X-RateLimit-Limit second value (monthly request-window cap)
	RemainingMonth float64 // X-RateLimit-Remaining second value (monthly request-window allowance)
	ResetSeconds   float64 // X-RateLimit-Reset second value (seconds until monthly window reset)

	HasLimitMonth     bool // true when X-RateLimit-Limit monthly value was parsed
	HasRemainingMonth bool // true when X-RateLimit-Remaining monthly value was parsed
	HasResetSeconds   bool // true when X-RateLimit-Reset monthly value was parsed
}

// Response is the top-level API response from Brave Search.
type Response struct {
	Web struct {
		Results []Result `json:"results"`
	} `json:"web"`

	// RateLimit is populated from response headers, not from JSON body.
	RateLimit *RateLimit `json:"-"`
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

	// Parse X-RateLimit-* headers for quota monitoring.
	out.RateLimit = parseRateLimitHeaders(resp.Header)
	ObserveRateLimitHeaders(resp.Header)

	return &out, nil
}

// parseRateLimitHeaders parses the Brave Search API rate limit headers
// and returns a RateLimit struct. Returns nil if no headers are present.
//
// Brave API response headers:
//
//	X-RateLimit-Limit:      "1, 15000"     (per-second, per-month)
//	X-RateLimit-Remaining:  "1, 1000"      (per-second, per-month)
//	X-RateLimit-Reset:      "1, 1419704"   (per-second seconds, monthly seconds)
//	X-RateLimit-Policy:     "1;w=1, 15000;w=2592000"
//
// We only extract the second (monthly) value from each multi-value header.
func parseRateLimitHeaders(h http.Header) *RateLimit {
	rl := &RateLimit{}
	var found bool

	if v := h.Get("X-RateLimit-Remaining"); v != "" {
		parts := strings.Split(v, ",")
		if len(parts) >= 2 {
			if val, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil && !math.IsNaN(val) && !math.IsInf(val, 0) && val >= 0 {
				rl.RemainingMonth = val
				rl.HasRemainingMonth = true
				found = true
			}
		}
	}

	if v := h.Get("X-RateLimit-Limit"); v != "" {
		parts := strings.Split(v, ",")
		if len(parts) >= 2 {
			if val, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil && !math.IsNaN(val) && !math.IsInf(val, 0) && val >= 0 {
				rl.LimitMonth = val
				rl.HasLimitMonth = true
				found = true
			}
		}
	}

	if v := h.Get("X-RateLimit-Reset"); v != "" {
		parts := strings.Split(v, ",")
		if len(parts) >= 2 {
			if val, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil && !math.IsNaN(val) && !math.IsInf(val, 0) && val >= 0 {
				rl.ResetSeconds = val
				rl.HasResetSeconds = true
				found = true
			}
		}
	}

	if !found {
		return nil
	}
	return rl
}

// ObserveRateLimitHeaders publishes the parsed monthly X-RateLimit-*
// header values to Prometheus gauges. When a header is absent or its
// value is invalid, the corresponding gauge is cleared so that stale
// values from a prior response are never reported as current.
func ObserveRateLimitHeaders(h http.Header) {
	rl := parseRateLimitHeaders(h)

	if rl == nil {
		// No parseable header at all — clear all three month gauges.
		metrics.BraveRateLimitRemaining.DeleteLabelValues("month")
		metrics.BraveRateLimitLimit.DeleteLabelValues("month")
		metrics.BraveRateLimitResetSeconds.DeleteLabelValues("month")
		return
	}

	if rl.HasRemainingMonth {
		metrics.BraveRateLimitRemaining.WithLabelValues("month").Set(rl.RemainingMonth)
	} else {
		metrics.BraveRateLimitRemaining.DeleteLabelValues("month")
	}
	if rl.HasLimitMonth {
		metrics.BraveRateLimitLimit.WithLabelValues("month").Set(rl.LimitMonth)
	} else {
		metrics.BraveRateLimitLimit.DeleteLabelValues("month")
	}
	if rl.HasResetSeconds {
		metrics.BraveRateLimitResetSeconds.WithLabelValues("month").Set(rl.ResetSeconds)
	} else {
		metrics.BraveRateLimitResetSeconds.DeleteLabelValues("month")
	}
}

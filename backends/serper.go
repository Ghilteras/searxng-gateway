package backends

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SerperBackend implements SearchBackend for Serper's Google Search API.
// Request/response shape follows the example on https://serper.dev/ (the docs
// URL https://serper.dev/docs returned 404 when this integration was written).
type SerperBackend struct {
	APIKey  string
	Timeout time.Duration
	BaseURL string
	client  *http.Client
}

func NewSerperBackend(apiKey string, timeout time.Duration) *SerperBackend {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &SerperBackend{APIKey: apiKey, Timeout: timeout, BaseURL: "https://google.serper.dev/search", client: &http.Client{Timeout: timeout}}
}

func (s *SerperBackend) Name() string      { return "serper" }
func (s *SerperBackend) IsAvailable() bool { return strings.TrimSpace(s.APIKey) != "" }

type serperAPIRequest struct {
	Query string `json:"q"`
	Num   int    `json:"num"`
}
type serperOrganicResult struct {
	Title    string `json:"title"`
	Link     string `json:"link"`
	Snippet  string `json:"snippet"`
	Position int    `json:"position"`
}
type serperAPIResponse struct {
	Organic []serperOrganicResult `json:"organic"`
}

func (s *SerperBackend) Search(opts SearchOptions) ([]SearchResult, error) {
	if !s.IsAvailable() {
		return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("serper API key not configured"), Code: ErrCodeUnavailable}
	}
	query := opts.Query
	if opts.Site != "" {
		query = fmt.Sprintf("site:%s %s", opts.Site, query)
	}
	num := opts.NumResults
	if num <= 0 {
		num = 10
	}
	payload, err := json.Marshal(serperAPIRequest{Query: query, Num: num})
	if err != nil {
		return nil, &BackendError{Backend: s.Name(), Err: err, Code: ErrCodeInvalidResponse}
	}
	req, err := http.NewRequestWithContext(searchContext(opts), http.MethodPost, s.BaseURL, bytes.NewReader(payload))
	if err != nil {
		return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("failed to create request: %w", err), Code: ErrCodeNetwork}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-API-KEY", s.APIKey)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("request failed: %w", err), Code: ErrCodeNetwork}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("failed to read response: %w", err), Code: ErrCodeInvalidResponse}
	}
	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("authentication failed (HTTP %d)", resp.StatusCode), Code: ErrCodeAuth}
		case http.StatusTooManyRequests:
			return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("rate limited (HTTP %d)", resp.StatusCode), Code: ErrCodeRateLimit}
		default:
			return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("HTTP %d", resp.StatusCode), Code: resp.StatusCode}
		}
	}
	var parsed serperAPIResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, &BackendError{Backend: s.Name(), Err: fmt.Errorf("failed to parse JSON: %w", err), Code: ErrCodeInvalidResponse}
	}
	results := make([]SearchResult, 0, len(parsed.Organic))
	for _, r := range parsed.Organic {
		results = append(results, SearchResult{Title: r.Title, URL: r.Link, Content: r.Snippet, Engine: s.Name(), Engines: []string{s.Name()}})
	}
	return results, nil
}

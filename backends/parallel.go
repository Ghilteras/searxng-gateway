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

// ParallelBackend implements SearchBackend for the Parallel Search API.
// Endpoint and request/response schema:
// https://docs.parallel.ai/search/search-quickstart
type ParallelBackend struct {
	APIKey  string
	Timeout time.Duration
	BaseURL string // overridable for testing
	client  *http.Client
}

// NewParallelBackend creates a new Parallel Search backend.
// A non-positive timeout falls back to 15s, matching the other premium backends.
func NewParallelBackend(apiKey string, timeout time.Duration) *ParallelBackend {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &ParallelBackend{
		APIKey:  apiKey,
		Timeout: timeout,
		BaseURL: "https://api.parallel.ai/v1/search",
		client:  &http.Client{Timeout: timeout},
	}
}

// Name returns the backend identifier.
func (p *ParallelBackend) Name() string {
	return "parallel"
}

// IsAvailable reports whether the backend is configured with an API key.
func (p *ParallelBackend) IsAvailable() bool {
	return strings.TrimSpace(p.APIKey) != ""
}

const (
	parallelMode        = "fast"
	parallelDefaultHits = 10
)

// parallelAdvancedSettings carries the documented result-count knob.
// max_results is documented to default to 10 when omitted.
type parallelAdvancedSettings struct {
	MaxResults int `json:"max_results"`
}

type parallelAPIRequest struct {
	Objective     string                   `json:"objective"`
	SearchQueries []string                 `json:"search_queries"`
	Mode          string                   `json:"mode"`
	Advanced      parallelAdvancedSettings `json:"advanced_settings"`
}

type parallelAPIResult struct {
	URL      string   `json:"url"`
	Title    string   `json:"title"`
	Excerpts []string `json:"excerpts"`
}

type parallelAPIResponse struct {
	Results []parallelAPIResult `json:"results"`
}

// Search performs a search against the Parallel Search API.
func (p *ParallelBackend) Search(opts SearchOptions) ([]SearchResult, error) {
	if !p.IsAvailable() {
		return nil, &BackendError{
			Backend: p.Name(),
			Err:     fmt.Errorf("parallel API key not configured"),
			Code:    ErrCodeUnavailable,
		}
	}

	query := opts.Query
	if opts.Site != "" {
		query = fmt.Sprintf("site:%s %s", opts.Site, query)
	}

	// Same default-and-clamp pattern exa uses: honour opts.NumResults when set,
	// otherwise fall back to 10 (also the API's documented default).
	count := opts.NumResults
	if count <= 0 {
		count = parallelDefaultHits
	}

	// search_queries is required by the API (at least one); send the single query.
	payload, err := json.Marshal(parallelAPIRequest{
		Objective:     query,
		SearchQueries: []string{query},
		Mode:          parallelMode,
		Advanced:      parallelAdvancedSettings{MaxResults: count},
	})
	if err != nil {
		return nil, &BackendError{Backend: p.Name(), Err: err, Code: ErrCodeInvalidResponse}
	}

	req, err := http.NewRequestWithContext(searchContext(opts), http.MethodPost, p.BaseURL, bytes.NewReader(payload))
	if err != nil {
		return nil, &BackendError{Backend: p.Name(), Err: fmt.Errorf("failed to create request: %w", err), Code: ErrCodeNetwork}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-api-key", p.APIKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, &BackendError{Backend: p.Name(), Err: fmt.Errorf("request failed: %w", err), Code: ErrCodeNetwork}
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &BackendError{Backend: p.Name(), Err: fmt.Errorf("failed to read response: %w", err), Code: ErrCodeInvalidResponse}
	}

	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, &BackendError{Backend: p.Name(), Err: fmt.Errorf("authentication failed (HTTP %d)", resp.StatusCode), Code: ErrCodeAuth}
		case http.StatusTooManyRequests:
			return nil, &BackendError{Backend: p.Name(), Err: fmt.Errorf("rate limited (HTTP %d)", resp.StatusCode), Code: ErrCodeRateLimit}
		default:
			return nil, &BackendError{Backend: p.Name(), Err: fmt.Errorf("HTTP %d", resp.StatusCode), Code: resp.StatusCode}
		}
	}

	var parsed parallelAPIResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, &BackendError{Backend: p.Name(), Err: fmt.Errorf("invalid JSON response"), Code: ErrCodeInvalidResponse}
	}

	results := make([]SearchResult, 0, len(parsed.Results))
	for _, r := range parsed.Results {
		results = append(results, SearchResult{
			Title:   r.Title,
			URL:     r.URL,
			Content: firstNonEmpty(r.Excerpts...),
			Engine:  p.Name(),
			Engines: []string{p.Name()},
		})
	}

	return results, nil
}

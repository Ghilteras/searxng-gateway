package backends

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// NewFromEnv creates a SearchBackend from environment variables.
// It reads <NAME>_API_KEY and any provider-specific env vars.
// PremiumPoolOrder fixes pool and round-robin order. Only these keyed backends
// are automatically enrolled; Jina and Bing remain outside the premium pool.
var PremiumPoolOrder = []string{"brave", "exa", "parallel", "tavily", "serper"}

// ConfiguredPool returns provider instances registered for eligibility
// reporting and the ordered subset with configured API keys for enrollment.
func ConfiguredPool(timeout time.Duration) ([]SearchBackend, []string, error) {
	all := make([]SearchBackend, 0, len(PremiumPoolOrder))
	enrolled := make([]string, 0, len(PremiumPoolOrder))
	for _, name := range PremiumPoolOrder {
		backend, err := NewFromEnv(name, timeout)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, backend)
		if backend.IsAvailable() {
			enrolled = append(enrolled, name)
		}
	}
	return all, enrolled, nil
}

// Supported names: brave, tavily, exa, parallel, serper.
func NewFromEnv(name string, timeout time.Duration) (SearchBackend, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	apiKey := os.Getenv(strings.ToUpper(name) + "_API_KEY")

	switch name {
	case "brave":
		if timeout == 0 {
			timeout = 15 * time.Second
		}
		return NewBraveBackend(apiKey, timeout), nil
	case "tavily":
		if timeout == 0 {
			timeout = 15 * time.Second
		}
		depth := os.Getenv("TAVILY_SEARCH_DEPTH")
		raw := os.Getenv("TAVILY_INCLUDE_RAW_CONTENT") == "true"
		answer := os.Getenv("TAVILY_INCLUDE_ANSWER") == "true"
		return NewTavilyBackend(apiKey, timeout, depth, raw, answer), nil
	case "exa":
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		mode := os.Getenv("EXA_MODE")
		mcpURL := os.Getenv("EXA_MCP_URL")
		mcpTool := os.Getenv("EXA_MCP_TOOL")
		numResults := 10
		// NewExaBackend signature: (mode, apiKey, timeout, mcpURL, mcpTool, numResults)
		return NewExaBackend(mode, apiKey, timeout, mcpURL, mcpTool, numResults), nil
	case "parallel":
		if timeout == 0 {
			timeout = 15 * time.Second
		}
		return NewParallelBackend(apiKey, timeout), nil
	case "serper":
		if timeout == 0 {
			timeout = 15 * time.Second
		}
		return NewSerperBackend(apiKey, timeout), nil
	default:
		return nil, fmt.Errorf("unknown backend: %q (available: brave, tavily, exa, parallel, serper)", name)
	}
}

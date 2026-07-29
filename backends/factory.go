package backends

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// NewFromEnv creates a SearchBackend from environment variables.
// It reads <NAME>_API_KEY and any provider-specific env vars.
// Supported names: brave, tavily, exa, jina, bing.
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
	case "jina":
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		allowKeyless := os.Getenv("JINA_ALLOW_KEYLESS") != "false"
		baseURL := os.Getenv("JINA_BASE_URL")
		return NewJinaBackend(apiKey, timeout, allowKeyless, baseURL), nil
	case "bing":
		if timeout == 0 {
			timeout = 10 * time.Second
		}
		// NewBingBackend is keyless — takes only timeout
		return NewBingBackend(timeout), nil
	default:
		return nil, fmt.Errorf("unknown backend: %q (available: brave, tavily, exa, jina, bing)", name)
	}
}

// ParseProviderList splits a comma-separated string into trimmed names.
func ParseProviderList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			names = append(names, t)
		}
	}
	return names
}

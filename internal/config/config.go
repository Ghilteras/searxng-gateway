package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr         string
	SearxngBackendURL  string
	BraveAPIKey        string
	FallbackTimeout    time.Duration
	SearxngTimeout     time.Duration
	BraveTimeout       time.Duration
	CacheSize          int
	CacheTTL           time.Duration
	LogLevel             string
	MetricsPath          string
	SufficientMinResults int // minimum results before T2 loop stops (default 10)
	FallbackProviders    []string // parsed from FALLBACK_PROVIDERS env var
	// Community-aligned: binary fallback + cooldown circuit breaker
	SearxngFailThreshold int           // consecutive failures before cooldown
	SearxngFailCooldown  time.Duration // duration of cooldown period
	BraveFailThreshold   int           // consecutive failures before Brave cooldown
	BraveFailCooldown    time.Duration // Brave cooldown duration
}

func Load() (*Config, error) {
	c := &Config{
		ListenAddr:            getEnv("LISTEN_ADDR", ":8080"),
		SearxngBackendURL:     getEnv("SEARXNG_BACKEND_URL", "http://searxng-primary:8080"),
		BraveAPIKey:           os.Getenv("BRAVE_API_KEY"),
		FallbackProviders:     parseProviderList(getEnv("FALLBACK_PROVIDERS", "brave")),
		FallbackTimeout:       time.Duration(getEnvInt("FALLBACK_TIMEOUT_SECONDS", 30)) * time.Second,
		SearxngTimeout:        time.Duration(getEnvInt("SEARXNG_TIMEOUT_SECONDS", 25)) * time.Second,
		BraveTimeout:          time.Duration(getEnvInt("BRAVE_TIMEOUT_SECONDS", 15)) * time.Second,
		CacheSize:             getEnvInt("CACHE_SIZE", 1000),
		CacheTTL:              time.Duration(getEnvInt("CACHE_TTL_SECONDS", 3600)) * time.Second,
		LogLevel:              getEnv("LOG_LEVEL", "info"),
		MetricsPath:           getEnv("METRICS_PATH", "/metrics"),
		SufficientMinResults:  getEnvInt("SUFFICIENT_MIN_RESULTS", 10),
		SearxngFailThreshold:  getEnvInt("SEARXNG_FAIL_THRESHOLD", 6),
		SearxngFailCooldown:   time.Duration(getEnvInt("SEARXNG_FAIL_COOLDOWN_SECONDS", 180)) * time.Second,
		BraveFailThreshold:    getEnvInt("BRAVE_FAIL_THRESHOLD", 3),
		BraveFailCooldown:     time.Duration(getEnvInt("BRAVE_FAIL_COOLDOWN_SECONDS", 300)) * time.Second,
	}
	return c, c.Validate()
}

func (c *Config) Validate() error {
	if c.SearxngFailThreshold < 1 {
		return fmt.Errorf("SEARXNG_FAIL_THRESHOLD must be >= 1, got %d", c.SearxngFailThreshold)
	}
	if c.SearxngFailCooldown < time.Second {
		return fmt.Errorf("SEARXNG_FAIL_COOLDOWN_SECONDS must be >= 1, got %v", c.SearxngFailCooldown)
	}
	if c.BraveFailThreshold < 1 {
		return fmt.Errorf("BRAVE_FAIL_THRESHOLD must be >= 1, got %d", c.BraveFailThreshold)
	}
	if c.BraveFailCooldown < time.Second {
		return fmt.Errorf("BRAVE_FAIL_COOLDOWN_SECONDS must be >= 1, got %v", c.BraveFailCooldown)
	}
	return nil
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getEnvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func parseProviderList(s string) []string {
	parts := strings.Split(s, ",")
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			names = append(names, strings.ToLower(t))
		}
	}
	return names
}

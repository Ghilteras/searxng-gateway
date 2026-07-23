package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ListenAddr         string
	SearxngBackendURL  string
	BraveAPIKey        string
	FallbackMinResults int
	FallbackMinEngines int
	FallbackTimeout    time.Duration
	SearxngTimeout     time.Duration
	BraveTimeout       time.Duration
	CacheSize          int
	CacheTTL           time.Duration
	LogLevel           string
	MetricsPath        string
}

func Load() (*Config, error) {
	c := &Config{
		ListenAddr:         getEnv("LISTEN_ADDR", ":8080"),
		SearxngBackendURL:  getEnv("SEARXNG_BACKEND_URL", "http://searxng-primary:8080"),
		BraveAPIKey:        os.Getenv("BRAVE_API_KEY"),
		FallbackMinResults: getEnvInt("FALLBACK_MIN_RESULTS", 5),
		FallbackMinEngines: getEnvInt("FALLBACK_MIN_ENGINES", 2),
		FallbackTimeout:    time.Duration(getEnvInt("FALLBACK_TIMEOUT_SECONDS", 30)) * time.Second,
		SearxngTimeout:     time.Duration(getEnvInt("SEARXNG_TIMEOUT_SECONDS", 25)) * time.Second,
		BraveTimeout:       time.Duration(getEnvInt("BRAVE_TIMEOUT_SECONDS", 15)) * time.Second,
		CacheSize:          getEnvInt("CACHE_SIZE", 1000),
		CacheTTL:           time.Duration(getEnvInt("CACHE_TTL_SECONDS", 3600)) * time.Second,
		LogLevel:           getEnv("LOG_LEVEL", "info"),
		MetricsPath:        getEnv("METRICS_PATH", "/metrics"),
	}
	return c, c.Validate()
}

func (c *Config) Validate() error {
	if c.BraveAPIKey == "" {
		return fmt.Errorf("BRAVE_API_KEY is required")
	}
	if c.FallbackMinResults < 1 {
		return fmt.Errorf("FALLBACK_MIN_RESULTS must be >= 1, got %d", c.FallbackMinResults)
	}
	if c.FallbackMinEngines < 1 {
		return fmt.Errorf("FALLBACK_MIN_ENGINES must be >= 1, got %d", c.FallbackMinEngines)
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

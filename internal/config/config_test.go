package config

import (
	"os"
	"testing"
	"time"
)

// saveEnv returns a snapshot of the current environment as ["KEY=VALUE", ...].
func saveEnv() []string {
	return os.Environ()
}

// restoreEnv restores the environment to a previously saved snapshot.
func restoreEnv(prev []string) {
	os.Clearenv()
	for _, kv := range prev {
		// os.Environ() returns "KEY=VALUE" pairs guaranteed to have '='
		eq := indexByte(kv, '=')
		if eq < 0 {
			continue
		}
		os.Setenv(kv[:eq], kv[eq+1:])
	}
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func TestLoadDefaults(t *testing.T) {
	prev := saveEnv()
	defer restoreEnv(prev)
	os.Clearenv()
	os.Setenv("BRAVE_API_KEY", "test-key")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":8080")
	}
	if cfg.SearxngBackendURL != "http://searxng-primary:8080" {
		t.Errorf("SearxngBackendURL = %q, want %q", cfg.SearxngBackendURL, "http://searxng-primary:8080")
	}
	if cfg.BraveAPIKey != "test-key" {
		t.Errorf("BraveAPIKey = %q, want %q", cfg.BraveAPIKey, "test-key")
	}
	if cfg.FallbackTimeout != 30*time.Second {
		t.Errorf("FallbackTimeout = %v, want 30s", cfg.FallbackTimeout)
	}
	if cfg.SearxngTimeout != 25*time.Second {
		t.Errorf("SearxngTimeout = %v, want 25s", cfg.SearxngTimeout)
	}
	if cfg.BraveTimeout != 15*time.Second {
		t.Errorf("BraveTimeout = %v, want 15s", cfg.BraveTimeout)
	}
	if cfg.CacheSize != 1000 {
		t.Errorf("CacheSize = %d, want 1000", cfg.CacheSize)
	}
	if cfg.CacheTTL != 3600*time.Second {
		t.Errorf("CacheTTL = %v, want 3600s", cfg.CacheTTL)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.MetricsPath != "/metrics" {
		t.Errorf("MetricsPath = %q, want %q", cfg.MetricsPath, "/metrics")
	}
	if cfg.SearxngFailThreshold != 6 {
		t.Errorf("SearxngFailThreshold = %d, want 6", cfg.SearxngFailThreshold)
	}
	if cfg.SearxngFailCooldown != 180*time.Second {
		t.Errorf("SearxngFailCooldown = %v, want 180s", cfg.SearxngFailCooldown)
	}
}

func TestLoadEnvOverride(t *testing.T) {
	prev := saveEnv()
	defer restoreEnv(prev)
	os.Clearenv()
	os.Setenv("BRAVE_API_KEY", "test-key")
	os.Setenv("SEARXNG_FAIL_THRESHOLD", "10")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.SearxngFailThreshold != 10 {
		t.Errorf("SearxngFailThreshold = %d, want 10", cfg.SearxngFailThreshold)
	}
	// Other fields should still get defaults
	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":8080")
	}
	if cfg.SearxngFailCooldown != 180*time.Second {
		t.Errorf("SearxngFailCooldown = %v, want 180s", cfg.SearxngFailCooldown)
	}
}

func TestLoadRequiresBraveKey(t *testing.T) {
	prev := saveEnv()
	defer restoreEnv(prev)
	os.Clearenv()

	if _, err := Load(); err == nil {
		t.Error("Load() expected error when BRAVE_API_KEY missing")
	}
}

func TestLoadInvalidThreshold(t *testing.T) {
	prev := saveEnv()
	defer restoreEnv(prev)
	os.Clearenv()
	os.Setenv("BRAVE_API_KEY", "test-key")
	os.Setenv("SEARXNG_FAIL_THRESHOLD", "0")

	if _, err := Load(); err == nil {
		t.Error("Load() expected error when SEARXNG_FAIL_THRESHOLD is 0")
	}
}

func TestLoadInvalidCooldown(t *testing.T) {
	prev := saveEnv()
	defer restoreEnv(prev)
	os.Clearenv()
	os.Setenv("BRAVE_API_KEY", "test-key")
	os.Setenv("SEARXNG_FAIL_COOLDOWN_SECONDS", "0")

	if _, err := Load(); err == nil {
		t.Error("Load() expected error when SEARXNG_FAIL_COOLDOWN_SECONDS is 0")
	}
}

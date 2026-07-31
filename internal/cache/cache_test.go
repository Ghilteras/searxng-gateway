package cache

import (
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	c, err := New(10, 0)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, ok := c.Get("missing"); ok {
		t.Error("Get on empty cache returned ok=true")
	}
	c.Set("k", "v")
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("Get after Set returned ok=false")
	}
	if got != "v" {
		t.Errorf("Get = %v, want %v", got, "v")
	}
}

func TestLen(t *testing.T) {
	c, _ := New(10, 0)
	if c.Len() != 0 {
		t.Errorf("Len on empty = %d, want 0", c.Len())
	}
	c.Set("a", 1)
	c.Set("b", 2)
	if c.Len() != 2 {
		t.Errorf("Len after 2 sets = %d, want 2", c.Len())
	}
}

func TestCacheTTLExpiry(t *testing.T) {
	c, _ := New(10, 50*time.Millisecond)
	c.Set("k", "v")
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit before TTL expiry")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Error("expected miss after TTL expiry")
	}
}

func TestCacheNoTTL(t *testing.T) {
	c, _ := New(10, 0)
	c.Set("k", "v")
	time.Sleep(10 * time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Error("expected hit with ttl=0 (no expiry)")
	}
}
